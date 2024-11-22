import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.LMDirichletSimilarity
import org.apache.lucene.search.similarities.Similarity

import java.io.File
import java.nio.file.Paths
import java.util.regex.Pattern
import kotlin.system.exitProcess


class QueryIndex {
    // Need to use the same analyzer and index directory throughout, so initialize them here
    val directory: Directory = FSDirectory.open(Paths.get("index"))
    var analyzer: Analyzer = CustomAnalyzer.builder()
        .withTokenizer("standard")
        .addTokenFilter("lowercase")
        .addTokenFilter("stop")
        .addTokenFilter("porterstem")
        .build()

    var similarity: Similarity
    var weights: Map<String, Map<String, Float>>
    lateinit var partialQueries: List<PartialQuery>
    init {
        this.similarity = LMDirichletSimilarity(500f)
        this.weights = mapOf(
            "title" to mapOf("headline" to 0.0f, "date" to 0.2f, "text" to 0.5f),
            "desc" to mapOf("headline" to 0.2f, "date" to 0.0f, "text" to 0.8f),
            "narr" to mapOf("headline" to 0.3f, "date" to 0.6f, "text" to 0.9f)
        )
    }


    data class QueryWithId(val num: String, val query: Query)
    
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

    data class PartialQuery(val num: String?, val title: String?, val desc: String?, val narr: String?)
    fun importQueries() {
        val queries = ArrayList<PartialQuery>()
        val file = File("queries/topics")
        if (file.isFile) {
            val content = file.readText()
            val querySeparator = Pattern.compile("(?<=<top>\\s)[\\s\\S]*?(?=</top>)").matcher(content)
            while (querySeparator.find()) {
                val rawQuery = querySeparator.group()
                val cleanQuery = sanitizeQuery(rawQuery)

                val num = findByTagAndProcessQuery(cleanQuery, "num", "title")
                val title = findByTagAndProcessQuery(cleanQuery, "title", "desc")
                val desc = findByTagAndProcessQuery(cleanQuery, "desc", "narr")
                val narr = findByTagAndProcessQuery(cleanQuery, "narr", " ")
                queries.add(PartialQuery(num, title, desc, narr))
            }
        }
        println("${queries.size} queries imported.")
        this.partialQueries = queries
    }


    fun processQueries(): List<QueryWithId> {
        val processedQueries = ArrayList<QueryWithId>()
        for (query in this.partialQueries) {
            // Specify the fields and weights for the MultiFieldQueryParser
            val fields = arrayOf("headline", "date", "text")
            val booleanQuery = BooleanQuery.Builder()

            query.title?.let {
                val titleQuery = MultiFieldQueryParser(fields, analyzer, weights["title"]).parse(it)
                booleanQuery.add(titleQuery, BooleanClause.Occur.SHOULD)
            }
            query.desc?.let {
                val descQuery = MultiFieldQueryParser(fields, analyzer, weights["desc"]).parse(it)
                booleanQuery.add(descQuery, BooleanClause.Occur.SHOULD)
            }
            query.narr?.let {
                val narrQuery = MultiFieldQueryParser(fields, analyzer, weights["narr"]).parse(it)
                booleanQuery.add(narrQuery, BooleanClause.Occur.SHOULD)
            }
            query.num?.let {
                processedQueries.add(QueryWithId(it, booleanQuery.build()))
            }
        }
//        println("${processedQueries.size} queries processed.")
        return processedQueries
    }    
            

    fun search(query: Query, isearcher : IndexSearcher ): Array<ScoreDoc> {
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
            var ind: Indexer? = null

            // index docs and prepare queries in parallel
            val startTime = System.currentTimeMillis()
            runBlocking {
                // use existing index unless -i flag is passed
                if (args.contains("-i")) {
                    println("Building index...")
                    ind = Indexer(qi.analyzer, qi.directory)

                    launch(Dispatchers.Default) { ind!!.indexAll() }
                } else {
                    println("Using existing index.")
                }
                qi.importQueries()
            }
            if (args.contains("-i")) {
                println("Indexing took ${(System.currentTimeMillis() - startTime) / 1000}s")
            }

            // shutdown the indexer if it was initialized
            ind?.run {
                shutdown()
            }

            // if args contains flag starting in -g, run grid search
            if (args.any { it.startsWith("-g")}) {
                GridSearch(qi).run {
                    if (args.contains("-gs")) {
                        searchSimilarities()
                        runTrecEval("grid_search/similarities", listOf("classic", "bm25", "lmd", "lmj"))
                    }
                    if (args.contains("-gw")) {
                        searchWeights()
                        runTrecEval("grid_search/weights", listOf("title", "desc", "narr"))
                    }
                    if (args.contains("-gc")) {
                        searchComparativeWeights()
                        runTrecEval("grid_search/weights/comp")
                    }
                    if (args.contains("-ga")) {
                        searchAnalyzers()
                        runTrecEval("grid_search/analyzers", listOf("tokenizer", "token_filter"))
                    }
                }
            } else {
                qi.run { runQueries(processQueries()) }
            }
            qi.directory.close()
            exitProcess(0)
        }
    }
}
