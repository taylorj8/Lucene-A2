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

import java.io.File
import java.util.regex.Pattern
import java.io.FileWriter
import java.io.BufferedWriter
import java.text.SimpleDateFormat
import kotlinx.coroutines.runBlocking

class Indexer(
    analyzer: Analyzer,
    directory: Directory
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
    
    private fun saveDocumentToFile(document: Document, filePath: String) {
        // Use FileWriter with append mode enabled
        BufferedWriter(FileWriter(filePath, true)).use { writer ->
            writer.write("Document Fields:\n")
            for (field in document.fields) {
                writer.write("${field.name()}: ${field.stringValue()}\n")
            }
            writer.write("\n") // Add a newline to separate documents
        }
    }

    private fun findByTagAndProcess(doc: String, tag: String): String? {
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

    fun indexAll(step: Int = 1, write: Boolean = false) = runBlocking {
        launch(Dispatchers.Default) { indexLaTimes(step, write) }
        launch(Dispatchers.Default) { indexFt(step, write) }
        launch(Dispatchers.Default) { indexFBis(step, write) }
        launch(Dispatchers.Default) { indexFr94(step, write) }
    }
    
    //as queries only contain years and no months, we extract the year only from each doc date field
    fun extractYear(date: String): String? {
        //  match 4 digits starting with '1'
        val regex = """\b1\d{3}\b""".toRegex()
        val match = regex.find(date)
    
        // if match is found return  otherwise null
        return match?.value
    }





    private fun indexLaTimes(step: Int = 1, write: Boolean = false) {
        println("Indexing LA Times documents...")
        File("test/la.txt").printWriter().use {}
        val subfolders = File("docs/latimes").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            runBlocking {
                for ((i, doc) in docs.withIndex()) {

                    val docId = findByTagAndProcess(doc, "DOCNO")
                    val headline = findByTagAndProcess(doc, "HEADLINE")
                    val text = findByTagAndProcess(doc, "TEXT")
                    val date = findByTagAndProcess(doc, "DATE")
                    val processedDate = date?.let {
                        extractYear(it) 
                    } ?: "null" 

                    val iDoc = Document().apply {
                        headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                        text?.let { add(TextField("text", it, Field.Store.YES)) }
                        docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                        processedDate?.let { add(TextField("date", it, Field.Store.YES)) }
                    }
                  
                    if (write && i < 50) {
                        saveDocumentToFile(iDoc, "test/la.txt")
                    }
                    iwriter.addDocument(iDoc)
                }
            }       
        }  
        println("${totalDocs / step} LA Times documents indexed.")
    } 
    
    //extract year from FT date 
    fun extractFTYear(date: String): String {
        val yearPrefix = date.substring(0, 2)
        val fullYear = "19$yearPrefix"
        return fullYear
    }

    private fun indexFt(step: Int = 1, write: Boolean = false) {
        println("Indexing Financial Times documents...")
        //clears the test file of any previous docs

        File("test/ft.txt").printWriter().use {}
        val subfolders = File("docs/ft").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for ((i, doc) in docs.withIndex()) {
                        if (i % step != 0) continue
                            val docId = findByTagAndProcess(doc, "DOCNO")
                            val headline = findByTagAndProcess(doc, "HEADLINE")
                            val text = findByTagAndProcess(doc, "TEXT")
                            val date = findByTagAndProcess(doc, "DATE")
                            val processedDate = date?.let {
                                extractFTYear(it) 
                            } ?: "null" 

                            val iDoc = Document().apply {
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                processedDate?.let { add(TextField("date", it, Field.Store.YES)) }
                                headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                                text?.let { add(TextField("text", it, Field.Store.YES)) }
                            }
                            // writes the first fifty docs found in each file to the test document
                            if (write && i < 50) {
                                saveDocumentToFile(iDoc, "test/ft.txt")
                            }
                            iwriter.addDocument(iDoc)
                    }
            }
        }
        println("${totalDocs / step} Financial Times documents indexed.")
    }


    private fun removePjgTagsFromDoc(doc: String): String {
        // Regular expression to match PJG tags, supporting multiline content within each tag
        val pjgTagPattern = Regex("<!-- PJG[\\s\\S]*?-->", RegexOption.DOT_MATCHES_ALL)
//        val matches = pjgTagPattern.findAll(doc)
        return doc.replace(pjgTagPattern, "")
    }



    private fun indexFr94(step: Int = 1, write: Boolean = false) {
        println("Indexing Federal Register 1994 documents...")
        //clears the test file of any previous docs

        File("test/fr94.txt").printWriter().use {}

        val subfolders = File("docs/fr94").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for ((i, doc) in docs.withIndex()) {
                        if (i % step != 0) continue
                            val newDoc = removePjgTagsFromDoc(doc)

                            val docId = findByTagAndProcess(newDoc, "DOCNO")
                            val header = findByTagAndProcess(newDoc, "DOCTITLE")

                            val summary = findByTagAndProcess(newDoc, "SUMMARY")
                            val body = findByTagAndProcess(newDoc,"SUPPLEM")
                            val textog = findByTagAndProcess(newDoc,"TEXT")
                            val sb = StringBuilder()
                            sb.append(summary).append(body).append(textog)
                            val text = sb.toString()

                            val date = findByTagAndProcess(newDoc,"DATE")
                            val processedDate = date?.let {
                                extractYear(it) 
                            } ?: "null" 

                            val iDoc = Document().apply {
                                header?.let { add(TextField("headline", it, Field.Store.YES)) }
                                add(TextField("text", text, Field.Store.YES))
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                processedDate?.let { add(TextField("date", it, Field.Store.YES)) }
                            }
                            if (write && i < 50) {
                                saveDocumentToFile(iDoc, "test/fr94.txt")
                            }
                            iwriter.addDocument(iDoc)
                        }
            }
        }

        println("${totalDocs / step} FR94 documents indexed.")
    }

    private fun indexFBis(step: Int = 1, write: Boolean = false) {
        println("Indexing Foreign Broadcast Information Services documents...")
        //clears the test file of any previous docs
        File("test/fbsi.txt").printWriter().use {}

        val subfolders = File("docs/fbis").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        runBlocking {
            subfolders?.forEach { folder ->
                    val docs = separateDocs(folder.absolutePath)
                    totalDocs += docs.size
                    for ((i, doc) in docs.withIndex()) {
                        if (i % step != 0) continue
                            val docId = findByTagAndProcess(doc, "DOCNO")
                            val header = findByTagAndProcess(doc, "TI")
                            val date = findByTagAndProcess(doc, "DATE1")
                            val processedDate = date?.let {
                                extractYear(it) 
                            } ?: "null" 
                            val text = findByTagAndProcess(doc, "TEXT")

                            val iDoc = Document().apply {
                                header?.let { add(TextField("headline", it, Field.Store.YES)) }
                                text?.let { add(TextField("text", it, Field.Store.YES)) }
                                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                                processedDate?.let { add(TextField("date", it, Field.Store.YES)) }
                            }
                            if (write && i < 50) {
                                saveDocumentToFile(iDoc, "test/fbsi.txt")
                            }
                            iwriter.addDocument(iDoc)
                    }
            }
        }
        println("${totalDocs / step} Foreign Broadcast Information Services documents indexed.")
    }


    fun shutdown() {
        iwriter.close()
    }
}
