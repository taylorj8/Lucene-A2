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


class QueryIndex {
    // Need to use the same analyzer and index directory throughout, so initialize them here
    private val directory: Directory = FSDirectory.open(Paths.get("index"))
    private val analyzer: Analyzer = CustomAnalyzer.builder()
        .withTokenizer("standard")
        .addTokenFilter("lowercase")
        .addTokenFilter("stop")
        .addTokenFilter("porterstem")
        .build()

    var similarity: Similarity
    private val iwriter: IndexWriter

    init {
        this.similarity = BM25Similarity()

        // create and configure an index writer
        val config = IndexWriterConfig(analyzer).apply {
            setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            setSimilarity(similarity)
        }
        this.iwriter = IndexWriter(directory, config)
    }

    fun separateDocs(directory: String): List<String> {
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
        println(docs.size.toString() + " documents separated from \"$directory\".")
        return docs
    }

    private fun findByTagAndProcess(doc: String, tag: String): String? {
        val matcher = Pattern.compile("(?<=<${tag}>)(.*?)(?=</${tag}>)").matcher(doc)
        if (matcher.find()) {
            return matcher.group().replace("<P>", "").replace("</P>", "").replace("\n", " ")
        }
        return null
    }

    fun indexLaTimes() {
        val docs = separateDocs("docs/latimes")
        for (doc in docs) {
            val docId = findByTagAndProcess(doc, "DOCID")
            val headline = findByTagAndProcess(doc, "HEADLINE")
            val text = findByTagAndProcess(doc, "TEXT")
            val section = findByTagAndProcess(doc, "SECTION")
            val byline = findByTagAndProcess(doc, "BYLINE")

            val iDoc = Document().apply {
                headline?.let { add(TextField("headline", it, Field.Store.YES)) }
                text?.let { add(TextField("text", it, Field.Store.YES)) }
                docId?.let { add(StringField("docId", it, Field.Store.YES)) }
                section?.let { add(StringField("section", it, Field.Store.YES)) }
                byline?.let { add(StringField("byline", it, Field.Store.YES)) }
            }
            iwriter.addDocument(iDoc)
        }
        println(docs.size.toString() + " LA Times documents indexed.")
    }


    // ASSIGNMENT 1 CODE
    fun buildIndex(fileName: String) {
        println("Indexing \"$fileName\"")

        // read in file and separate into documents
        val content = File(fileName).readText()
        val divisor = Pattern.compile("\\.I \\d+[\\s\\S]*?(?=\\.I |$)").matcher(content)
        val sections = ArrayList<String>()
        while (divisor.find()) {
            sections.add(divisor.group())
        }

        // make regex patterns to match the sections of each doc
        val idPattern = Pattern.compile("(?<=\\.I )\\d+")
        val authorPattern = Pattern.compile("(?<=\\.A\n).*")
        val titlePattern = Pattern.compile("(?<=\\.T\n)(.*?)(?=\n\\.)", Pattern.DOTALL)
        val textPattern = Pattern.compile("(?<=\\.W\n)(.*?)(?=$)", Pattern.DOTALL)
        
        for (section in sections) {
            // match each attribute with regex
            val idMatcher = idPattern.matcher(section).apply { find() }
            val authorMatcher = authorPattern.matcher(section).apply { find() }
            val titleMatcher = titlePattern.matcher(section).apply { find() }
            val textMatcher = textPattern.matcher(section).apply { find() }

            // index the doc
            val doc = Document().apply {
                add(TextField("title", titleMatcher.group(), Field.Store.YES))
                add(TextField("content", textMatcher.group(), Field.Store.YES))
                add(StringField("id", idMatcher.group(), Field.Store.YES))
                add(StringField("author", authorMatcher.group(), Field.Store.YES))
            }

            iwriter.addDocument(doc)
        }
        println(sections.size.toString() + " documents indexed.")
    }

    // ASSIGNMENT 1 CODE
    fun importQueries(fileName: String): List<String> {
        val content = File(fileName).readText()
        val queryMatcher = Pattern.compile("(?<=\\.W)(.*?)(?=\\.I|$)", Pattern.DOTALL).matcher(content)

        val queries = ArrayList<String>()
        while (queryMatcher.find()) {
            queries.add(queryMatcher.group().replace("\\s+".toRegex(), " ").replace("\\?".toRegex(), ""))
        }

        println(queries.size.toString() + " queries imported.")
        return queries
    }

    // ASSIGNMENT 1 CODE
    fun correctQrel(fileName: String) {
        // create file to store corrected qrel if it doesn't exist
        File("cran/corcranqrel").let { qrelFile ->
            if (qrelFile.createNewFile()) {
                File(fileName).forEachLine() { line ->
                    val judgements = line.split(" +".toRegex()).filter { line != "" }
                    val ranking = if(judgements[2] == "-1") 5 else judgements[2]
                    qrelFile.appendText(judgements[0] + " 0 " + judgements[1] + " " + ranking + "\n")
                }
                println("Corrected qrel file.")
            }
        }
    }

    // ASSIGNMENT 1 CODE
    fun runQueries(queries: List<String>) {
        // create file to store results, or clear it if it exists
        val simName = similarity::class.simpleName
        val resultsFile = File("results/${simName}_results.txt")
        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")
        }

        for ((i, query) in queries.withIndex()) {
            // Use IndexSearcher to retrieve some arbitrary document from the index
            val hits = search(query)

            // write hits to file compatible with trec_eval
            for ((j, hit) in hits.withIndex()) {
                resultsFile.appendText((i + 1).toString() + " Q0 " + (hit.doc + 1) + " " + (j + 1) + " " + hit.score + " " + simName + "\n")
            }
        }
        println("Results saved to file.")
    }

    // ASSIGNMENT 1 CODE
    fun search(searchTerm: String): Array<ScoreDoc> {
        val ireader = DirectoryReader.open(directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }
        val parser = MultiFieldQueryParser(
            arrayOf("title", "content"),
            analyzer,
            mapOf("title" to 0.7f, "content" to 1f)
        )

        val query = parser.parse(searchTerm)
        val hits = isearcher.search(query, 50).scoreDocs

        // Make sure we actually found something
        if (hits.isEmpty()) {
            println("Failed to retrieve a document for query \"${searchTerm}\"")
            return emptyArray()
        }
        ireader.close()
        return hits
    }

    fun shutdown() {
        iwriter.close()
        directory.close()
    }
    
    companion object {
        @JvmStatic fun main(args: Array<String>) {
//            if (args.size !in 1..3) {
//                println("Expected Arguments.")
//                exitProcess(1)
//            }

            val qi = QueryIndex()
            // use existing index unless -i flag is passed
            if (args.isNotEmpty() && args[0] == "-i") {
                qi.indexLaTimes()
                val ftDocs = qi.separateDocs("docs/ft")
                val frDocs = qi.separateDocs("docs/fr94")
                val fbDocs = qi.separateDocs("docs/fbis")
            } else {
                println("Using existing index.")
            }

            qi.shutdown()
            exitProcess(0)

            // ASSIGNMENT 1 CODE
//            qi.buildIndex(args[0])
//
//            if (args.size >= 2) {
//                val queries = qi.importQueries(args[1])
//                qi.runQueries(queries)
//
//                // change similarity score and re-run queries
//                qi.similarity = ClassicSimilarity()
//                qi.buildIndex(args[0])
//                qi.runQueries(queries)
//                if (args.size == 3) {
//                    qi.correctQrel(args[2])
//                }
//
//                qi.shutdown()
//                exitProcess(0)
//            }
//
//            println("No arguments passed, running in search mode. Press enter with no search term to exit.")
//            while (true) {
//                print("🔍: ")
//                val searchTerm = readlnOrNull()
//                if (searchTerm.isNullOrEmpty()) {
//                    print("Shutting down\n")
//                    qi.shutdown()
//                    exitProcess(0)
//                }
//
//                val hits = qi.search(searchTerm)
//                print("${hits.size} results found:\n")
//                for (hit in hits) {
//                    print(hit.doc.toString() + "\n")
//                }
//            }
        }
    }
}
