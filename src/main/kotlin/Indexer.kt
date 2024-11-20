import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StringField

import org.apache.lucene.search.similarities.Similarity

import java.io.File
import java.util.regex.Pattern

import kotlinx.coroutines.runBlocking

class Indexer(private val analyzer: Analyzer,
            private val directory: Directory
){
    private val iwriter: IndexWriter

    init {
        // create and configure an index writer
        val config = IndexWriterConfig(analyzer).apply {
            setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            setSimilarity(similarity)
        }
        this.iwriter = IndexWriter(directory, config)
    }
    private fun separateDocs(directory: String): List<String> {
        // loop through each file in the directory
        val docs = ArrayList<String>()
        File(directory).walk().forEach {
            if (it.isFile) {
                
                val content = it.readText()
                val docSeparator = Pattern.compile("(?<=<DOC>\\s)[\\s\\S]*?(?=</DOC>)").matcher(content)
                while (docSeparator.find()) {
                    docs.add(docSeparator.group())
                }
            }
        }
        return docs
    }
    

    fun findByTagAndProcess(doc: String, tag: String): String? {
        val matcher = Pattern.compile("(?<=<${tag}>)([\\s\\S]*?)(?=</${tag}>)").matcher(doc)
        if (matcher.find()) {
            return matcher.group().replace(Regex("<!--.*?-->"), " ") 
            .replace("<P>", "")
            .replace("</P>", "")
            .replace(Regex("<.*?F.*?>"), " ") // Replaces anything matching <*F*> with a space
            .replace("\n", " ")
        }
        return null
    }

    fun indexLaTimes() {
        println("Indexing LA Times documents...")
        val subfolders = File("docs/latimes").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            runBlocking {
                for (doc in docs) {
                    launch(Dispatchers.Default) {
                        val docId = findByTagAndProcess(doc, "DOCID")
                        val headline = findByTagAndProcess(doc, "HEADLINE")
                        val text = findByTagAndProcess(doc, "TEXT")
                        //Optional
                        val date = findByTagAndProcess(doc, "DATE")
                        //val section = findByTagAndProcess(doc, "SECTION")
                        //val byline = findByTagAndProcess(doc, "BYLINE")

                        val iDoc = Document().apply {
                            headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                            text?.let { add(TextField("text", it, Field.Store.YES)) }
                            docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                            date?.let { add(TextField("date", it, Field.Store.YES)) }
                            //Optional
                            //section?.let { add(StringField("section", it, Field.Store.YES)) }
                            //byline?.let { add(StringField("byline", it, Field.Store.YES)) }
                        }
                        iwriter.addDocument(iDoc)
                    }
                }
            }
        }  
        println("$totalDocs LA Times documents indexed.")
    } 

    fun indexFt() {
        println("Indexing Financial Times documents...")
        val subfolders = File("docs/ft").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                launch(Dispatchers.Default) {
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for (doc in docs) {
                        launch(Dispatchers.Default) {
                            val docId = findByTagAndProcess(doc, "DOCNO")
                            val headline = findByTagAndProcess(doc, "HEADLINE")
                            val text = findByTagAndProcess(doc, "TEXT")
                            val date = findByTagAndProcess(doc, "DATE")

                            val iDoc = Document().apply {
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                date?.let { add(TextField("date", it, Field.Store.YES)) }
                                headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                                text?.let { add(TextField("text", it, Field.Store.YES)) }
                            }
                            iwriter.addDocument(iDoc)
                        }
                    }
                }
            }
        }
        println("$totalDocs Financial Times documents indexed.")
    }

    fun removePjgTagsFromDoc(doc: String): String {
        // Regular expression to match PJG tags, supporting multiline content within each tag
        val pjgTagPattern = Regex("<!-- PJG[\\s\\S]*?-->", RegexOption.DOT_MATCHES_ALL)
        val matches = pjgTagPattern.findAll(doc)
        return doc.replace(pjgTagPattern, "")
    }

    fun extractDate(text: String): String? {
        // Regular expression to match a date in the format "Month Day, Year"
        val datePattern = Regex("\\b(January|February|March|April|May|June|July|August|September|October|November|December) \\d{1,2}, \\d{4}\\b")

        // Find the first match of the date pattern in the text
        val match = datePattern.find(text)

        return match?.value
    }

    fun indexFr94() {
        println("Indexing Federal Register 1994 documents...")
        val subfolders = File("docs/fr94").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                launch(Dispatchers.Default) {
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for (doc in docs) {
                        launch(Dispatchers.Default) {
                            val newDoc = removePjgTagsFromDoc(doc)
                            // print(newdoc)

                            val docId = findByTagAndProcess(newDoc, "DOCNO")
                            val header = findByTagAndProcess(newDoc, "DOCTITLE")
                            val summary = findByTagAndProcess(newDoc, "SUMMARY")
                            val body =  findByTagAndProcess(newDoc,"SUPPLEM")
                            val sb = StringBuilder()
                            sb.append(summary).append(body)
                            val text = sb.toString();
                            val date = findByTagAndProcess(newDoc,"DATE")
                            val processedDate = date?.let { extractDate(it) }
                            //print("printing date");
                            // print(processedDate+"\n");

                            val iDoc = Document().apply {
                                header?.let { add(TextField("headline", it, Field.Store.YES)) }
                                add(TextField("text", text, Field.Store.YES))
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                processedDate?.let { add(TextField("date", it, Field.Store.YES)) }
                            }
                            iwriter.addDocument(iDoc)
                        }
                    }
                }
            }
        }

        println("$totalDocs FR94 documents indexed.")

    } 

    fun indexFBis() {
        println("Indexing Foreign Broadcast Information Services documents...")
        val subfolders = File("docs/fbis").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                launch(Dispatchers.Default) {
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for (doc in docs) {
                        launch(Dispatchers.Default) {
                            val docId = findByTagAndProcess(doc, "DOCNO")
                            val header = findByTagAndProcess(doc, "TI")
                            val date = findByTagAndProcess(doc, "DATE1")
                            val text = findByTagAndProcess(doc, "TEXT")

                            val iDoc = Document().apply {
                                header?.let { add(TextField("headline", it, Field.Store.YES)) }
                                text?.let { add(TextField("text", it, Field.Store.YES)) }
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                date?.let { add(TextField("date", it, Field.Store.YES)) }
                            }
                            iwriter.addDocument(iDoc)
                        }
                    }
                }
            }
        }

        println("$totalDocs Foreign Broadcast Information Services documents indexed.")
    }
    
    fun shutdown() {
        iwriter.close()
    }
}
