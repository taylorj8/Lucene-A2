import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StringField
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.BM25Similarity
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser

import org.apache.lucene.search.similarities.Similarity

import java.io.File
import java.nio.file.Paths
import java.util.regex.Pattern
import kotlin.system.exitProcess
import java.text.SimpleDateFormat

class Indexer(private val analyzer: Analyzer, 
            private val directory: Directory,
            private val similarity : Similarity
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



    fun normalizeDate(dateStr: String): String? {
        val possibleFormats = listOf(
            SimpleDateFormat("yyyyMMdd", Locale.ENGLISH),
            SimpleDateFormat("dd MMM yy", Locale.ENGLISH),
            SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH)
        )
        val targetFormat = SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH) // Standard ISO format

        for (format in possibleFormats) {
            try {
                val parsedDate = format.parse(dateStr.trim())
                return targetFormat.format(parsedDate)
            } catch (e: Exception) {

            }
        }
        return null
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
        val subfolders = File("docs/latimes").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            for (doc in docs) {
                val docId = findByTagAndProcess(doc, "DOCNO")
                val headline = findByTagAndProcess(doc, "HEADLINE")
                val text = findByTagAndProcess(doc, "TEXT")
                val date = findByTagAndProcess(doc, "DATE")
                val normalizedDate = date?.let { normalizeDate(it) }

                val iDoc = Document().apply {
                    docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                    normalizedDate?.let {
                        val dateField = TextField("date", it, Field.Store.YES)
                        dateField.boost = 2.0f
                        add(dateField)
                    }
                    headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                    text?.let { add(TextField("text", it, Field.Store.YES)) }
                }
                iwriter.addDocument(iDoc)
            }
        }
        println(totalDocs.toString() + " LA Times documents indexed.")
    }


    fun indexFt() {
        val subfolders = File("docs/ft").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            for (doc in docs) {
                val docId = findByTagAndProcess(doc, "DOCNO")
                val headline = findByTagAndProcess(doc, "HEADLINE")
                val text = findByTagAndProcess(doc, "TEXT")
                val date = findByTagAndProcess(doc, "DATE")
                val normalizedDate = date?.let { normalizeDate(it) }

                val iDoc = Document().apply {
                    docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                    normalizedDate?.let {
                        val dateField = TextField("date", it, Field.Store.YES)
                        dateField.boost = 2.0f
                        add(dateField)
                    }
                    headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                    text?.let { add(TextField("text", it, Field.Store.YES)) }
                }
                iwriter.addDocument(iDoc)
            }
        }
        println(totalDocs.toString() + " Financial Times documents indexed.")
    }


    fun removePjgTagsFromDoc(doc: String): String {
        // Regular expression to match PJG tags, supporting multiline content within each tag
        val pjgTagPattern = Regex("<!-- PJG[\\s\\S]*?-->", RegexOption.DOT_MATCHES_ALL);
        val matches = pjgTagPattern.findAll(doc)
        return doc.replace(pjgTagPattern, "")
    }

    fun extractDate(text: String): String? {
        // Regular expression to match a date in the format "Month Day, Year"
        val datePattern = Regex("\\b(January|February|March|April|May|June|July|August|September|October|November|December) \\d{1,2}, \\d{4}\\b")

        // Find the first match of the date pattern in the text
        val match = datePattern.find(text)

        return if (match != null) {
           // println("Found date: ${match.value}")  // Print the found date for verification
            match.value  // Return the matched date
        } else {
           // println("No date found.")
            null  // Return null if no match is found
        }
    }




    fun indexFr94() {
        val subfolders = File("docs/fr94").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            for (doc in docs) {
                val docId = findByTagAndProcess(doc, "DOCNO")
                val headline = findByTagAndProcess(doc, "HEADLINE")
                val text = findByTagAndProcess(doc, "TEXT")
                val date = findByTagAndProcess(doc, "DATE")
                val normalizedDate = date?.let { normalizeDate(it) }

                val iDoc = Document().apply {
                    docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                    normalizedDate?.let {
                        val dateField = TextField("date", it, Field.Store.YES)
                        dateField.boost = 2.0f
                        add(dateField)
                    }
                    headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                    text?.let { add(TextField("text", it, Field.Store.YES)) }
                }
                iwriter.addDocument(iDoc)
            }
        }
        println(totalDocs.toString() + " FR94 documents indexed.")
    }


    fun indexFBis() {
        val subfolders = File("docs/fbis").listFiles { file -> file.isDirectory }
        var totalDocs = 0
        subfolders?.forEach { folder ->
            val docs = separateDocs(folder.absolutePath)
            totalDocs += docs.size
            for (doc in docs) {
                val docId = findByTagAndProcess(doc, "DOCNO")
                val headline = findByTagAndProcess(doc, "TI") // Assuming "TI" is the tag for the title/headline
                val text = findByTagAndProcess(doc, "TEXT")
                val date = findByTagAndProcess(doc, "DATE")
                val normalizedDate = date?.let { normalizeDate(it) }

                val iDoc = Document().apply {
                    docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                    normalizedDate?.let {
                        val dateField = TextField("date", it, Field.Store.YES)
                        dateField.boost = 2.0f
                        add(dateField)
                    }
                    headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                    text?.let { add(TextField("text", it, Field.Store.YES)) }
                }
                iwriter.addDocument(iDoc)
            }
        }
        println(totalDocs.toString() + " FBIS documents indexed.")
    }


    fun shutdown() {
        iwriter.close()
    }



}
