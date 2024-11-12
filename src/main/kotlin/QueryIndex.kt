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
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.queryparser.classic.QueryParser

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
    init {
        this.similarity = BM25Similarity()
    }

    data class QueryWithId(val num: String, val query: BooleanQuery)

    fun importQueries(ind: Indexer): List<QueryWithId> {
        val queries = ArrayList<QueryWithId>()
        val file = File("queries/topics")
        if (file.isFile) {
            val content = file.readText()
            val querySeparator = Pattern.compile("(?<=<top>\\s)[\\s\\S]*?(?=</top>)").matcher(content)
            while (querySeparator.find()) {
                val rawQuery = querySeparator.group()
    
                val num = ind.findByTagAndProcess(rawQuery, "num")
                val title = ind.findByTagAndProcess(rawQuery, "title")
                val desc = ind.findByTagAndProcess(rawQuery, "desc")
                val narr = ind.findByTagAndProcess(rawQuery, "narr")
                
                // Specify the fields and weights for the MultiFieldQueryParser
                val fields = arrayOf("headline", "date", "text")
                val fieldWeightsTitle = mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f)
                val fieldWeightsDesc = mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f)
                val fieldWeightsNarr = mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f)
    
                val booleanQuery = BooleanQuery.Builder()
    
                title?.let {
                    val titleQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsTitle).parse(it)
                    booleanQuery.add(titleQuery, BooleanClause.Occur.SHOULD)
                }
                desc?.let {
                    val descQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsDesc).parse(it)
                    booleanQuery.add(descQuery, BooleanClause.Occur.SHOULD)
                }
                narr?.let {
                    val narrQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsNarr).parse(it)
                    booleanQuery.add(narrQuery, BooleanClause.Occur.SHOULD)
                }
                
                num?.let {
                    queries.add(QueryWithId(it, booleanQuery.build()))
                }            
            }
        }
        println("${queries.size} queries prepared.")
        return queries
    }
        
                
    fun search(query: BooleanQuery): Array<ScoreDoc> {
        val ireader = DirectoryReader.open(directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }
        val hits = isearcher.search(query, 50).scoreDocs

        // Make sure we actually found something
        if (hits.isEmpty()) {
            println("Failed to retrieve documents for query \"$query\"")
            return emptyArray()
        }
        ireader.close()
        return hits
    }


    
    // ASSIGNMENT 1 CODE
    fun correctQrel(fileName: String) {
        // create file to store corrected qrel if it doesn't exist
        File("cran/corcranqrel").let { qrelFile ->
            if (qrelFile.createNewFile()) {
                File(fileName).forEachLine { line ->
                    val judgements = line.split(" +".toRegex()).filter { line != "" }
                    val ranking = if(judgements[2] == "-1") 5 else judgements[2]
                    qrelFile.appendText(judgements[0] + " 0 " + judgements[1] + " " + ranking + "\n")
                }
                println("Corrected qrel file.")
            }
        }
    }

    fun runQueries(queries: List<QueryWithId>) {
        // Create file to store results, or clear it if it exists
        val simName = similarity::class.simpleName
        val resultsFile = File("results/${simName}_results.txt")
        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")  // Clear the file if it exists
        }
    
        for ((index, queryWithId) in queries.withIndex()) {
            val query = queryWithId.query
            val queryId = queryWithId.num
            // Use IndexSearcher to retrieve documents from the index based on BooleanQuery
            val hits = search(query)
            // Write hits to file compatible with trec_eval
            for ((j, hit) in hits.withIndex()) {
                resultsFile.appendText("${queryId} Q0 ${hit.doc + 1} ${j + 1} ${hit.score} $simName\n")
            }
        }
        println("Results saved to file.")
    }
    

    companion object {
        @JvmStatic fun main(args: Array<String>) {
//            if (args.size !in 1..3) {
//                println("Expected Arguments.")
//                exitProcess(1)
//            }
            val qi = QueryIndex()
            val ind = Indexer(qi.analyzer, qi.directory, qi.similarity)

            
            // use existing index unless -i flag is passed
            if (args.isNotEmpty() && args[0] == "-i") {
                // Call the index functions
                ind.indexLaTimes()
                ind.indexFt()
                ind.indexFBis()
                //ind.indexFr94()
            } else {
                println("Using existing index.")
            }

            ind.shutdown()
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
