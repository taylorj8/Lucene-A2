import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.BM25Similarity
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.similarities.Similarity

import java.io.File
import java.nio.file.Paths
import java.util.regex.Pattern
import kotlin.system.exitProcess


class QueryIndex {
    // Need to use the same analyzer and index directory throughout, so initialize them here
    val directory: Directory = FSDirectory.open(Paths.get("index"))
    private val analyzer: Analyzer = CustomAnalyzer.builder()
        .withTokenizer("standard")
        .addTokenFilter("lowercase")
        .addTokenFilter("stop")
        .addTokenFilter("porterstem")
        .build()

    var similarity: Similarity
    var weights: Map<String, Map<String, Float>>
    init {
        this.similarity = BM25Similarity()
        this.weights = mapOf(
            "title" to mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f),
            "desc" to mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f),
            "narr" to mapOf("headline" to 0.8f, "date" to 0.2f, "text" to 1f)
        )
    }

    data class QueryWithId(val num: String, val query: BooleanQuery)
    
    fun sanitizeQuery(input: String): String {
        // Replace newlines and tabs with spaces
        var sanitized = input.replace("\n", " ").replace("\t", " ")
    
        // Escape special characters
        val specialChars = arrayOf("+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", "\\", "/")
        for (char in specialChars) {
            sanitized = sanitized.replace(char, "")
        }
    
        return sanitized
    }

    private fun findByTagAndProcessQuery(doc: String, tag1: String, tag2: String): String? {
    
        val pattern = if(tag1 == "narr"){
            "(?<=<${tag1}>)([\\s\\S]*?)$"
        } else {
            "(?<=<${tag1}>)([\\s\\S]*?)(?=<${tag2}>)"
        }
        val matcher = Pattern.compile(pattern).matcher(doc)
            
        if (matcher.find()) {
            return matcher.group().replace(Regex("^\\s*(Number\\:|Description\\:|Narrative\\:)\\s*"), "")   
        }
        return null
    }


    fun importQueries(): List<QueryWithId> {
        val queries = ArrayList<QueryWithId>()
        val file = File("queries/topics")
        if (file.isFile) {
            
            val content = file.readText()
            val querySeparator = Pattern.compile("(?<=<top>\\s)[\\s\\S]*?(?=</top>)").matcher(content)
            while (querySeparator.find()) {
                
                val rawQuery = querySeparator.group()
                val cleanQuery = sanitizeQuery(rawQuery)
                val num = findByTagAndProcessQuery(cleanQuery, "num", "title" )
                val title = findByTagAndProcessQuery(cleanQuery, "title", "desc")
                val desc = findByTagAndProcessQuery(cleanQuery, "desc", "narr")
                val narr = findByTagAndProcessQuery(cleanQuery, "narr", " " )

                // Specify the fields and weights for the MultiFieldQueryParser
                val fields = arrayOf("headline", "date", "text")
    
                val booleanQuery = BooleanQuery.Builder()
    
                title?.let {
                    val titleQuery = MultiFieldQueryParser(fields, analyzer, weights["title"]).parse(it)
                    booleanQuery.add(titleQuery, BooleanClause.Occur.SHOULD)
                }
                desc?.let {
                    val descQuery = MultiFieldQueryParser(fields, analyzer, weights["desc"]).parse(it)
                    booleanQuery.add(descQuery, BooleanClause.Occur.SHOULD)
                }                
                narr?.let {
                    val narrQuery = MultiFieldQueryParser(fields, analyzer, weights["narr"]).parse(it)
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
            

    fun search(query: BooleanQuery,  isearcher : IndexSearcher ): Array<ScoreDoc> {
        val hits = isearcher.search(query, 50).scoreDocs

        // Make sure we actually found something
        if (hits.isEmpty()) {
            println("Failed to retrieve documents for query \"$query\"")
            return emptyArray()
        }
        return hits
    }


    fun runQueries(queries: List<QueryWithId>, ) {
        // Create file to store results, or clear it if it exists
        val simName = similarity::class.simpleName
        val resultsFile = File("results/${simName}_results.txt")

        val ireader = DirectoryReader.open(directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }

        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")  // Clear the file if it exists
        }
    
        for (queryWithId in queries) {
            val query = queryWithId.query
            val queryId = queryWithId.num
            // Use IndexSearcher to retrieve documents from the index based on BooleanQuery
            val hits = search(query, isearcher)
   
            // Write hits to file compatible with trec_eval
            for ((j,hit) in hits.withIndex()) {
                val doc = isearcher.doc(hit.doc)
                val docId = doc["docId"]
                resultsFile.appendText("$queryId Q0 $docId ${j+1} ${hit.score} $simName\n")
            }
        }
        ireader.close()
        println("Results saved to file.")
    }


    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val qi = QueryIndex()
            var queries: List<QueryWithId>
            var ind: Indexer? = null

            // index docs and prepare queries in parallel
            val startTime = System.currentTimeMillis()
            runBlocking {
                // use existing index unless -i flag is passed
                if (args.contains("-i")) {
                    println("Building index...")
                    ind = Indexer(qi.analyzer, qi.directory)
                    // time the indexing process

                    launch(Dispatchers.Default) { ind!!.indexLaTimes() }
                    launch(Dispatchers.Default) { ind!!.indexFt() }
                    launch(Dispatchers.Default) { ind!!.indexFBis() }
                    launch(Dispatchers.Default) { ind!!.indexFr94() }

                } else {
                    println("Using existing index.")
                }
                queries = qi.importQueries()
            }
            val endTime = System.currentTimeMillis()
            println("Indexing took ${(endTime - startTime) / 1000}s")

            // shutdown the indexer if it was initialized
            ind?.run {
                shutdown()
            }

            if (args.contains("-g")) {
                println("Running Grid Search")
                val gridSearch = GridSearch(qi, queries)
                gridSearch.searchSimilarities()
//                gridSearch.searchWeights()
                gridSearch.runTrecEval()
            } else {
                qi.runQueries(queries)

                qi.directory.close()
            }
            exitProcess(0)
        }
    }
}
