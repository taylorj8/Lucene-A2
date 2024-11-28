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
import org.apache.lucene.search.similarities.BM25Similarity
import org.apache.lucene.search.similarities.Similarity
import java.io.BufferedReader

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
    var boosts: Map<String, Float>
    lateinit var partialQueries: List<PartialQuery>
    init {
        this.similarity = BM25Similarity(0.6f, 0.7f)
        this.weights = mapOf(
            "title" to mapOf("headline" to 0.2f, "date" to 0.0f, "text" to 1.0f),
            "desc" to mapOf("headline" to 0.2f, "date" to 0.0f, "text" to 0.6f),
            "narr" to mapOf("headline" to 0.2f, "date" to 1.0f, "text" to 0.8f),
            "date" to mapOf("headline" to 0.2f, "date" to 1.0f, "text" to 0.2f)
        )
        this.boosts = mapOf("title" to 0.8f, "desc" to 0.4f, "narr" to 0.2f, "date" to 0.0f)
    }

    data class QueryWithId(val num: String, val query: Query)
    private fun sanitizeQuery(input: String): String {
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
            return matcher.group().replace(Regex("^\\s*(Number:|Description:|Narrative:)\\s*"), "")
        }
        return null
    }

    private fun getDates(text: String): String {
        // Regular expression to match the patterns
        val regex = Regex("""\b(17\d{2}|18\d{2}|19\d{2}|20\d{2})(s?)\b""", RegexOption.IGNORE_CASE)

        val results = mutableListOf<String>()

        regex.findAll(text).forEach { match ->
            val matchedValue = match.value

            // check if ends in an "s"
            if (matchedValue.endsWith("s", ignoreCase = true)) {
                // get the decade e,g the 90s
                val decadeStart = matchedValue.substring(0, 4).toInt()
                // add all years in that decade e.g 90, 91, 92, 93, 94.....
                results.addAll((decadeStart..decadeStart + 9).map { it.toString() })
            } else {
                // else add the single year
                results.add(matchedValue)
            }
        }
        return if (results.isEmpty()) "null" else results.joinToString(", ")
    }

    data class PartialQuery(val num: String?, val title: String?, val desc: String?, val narr: String?, val date: String?)
    fun importQueries() {
        val queries = ArrayList<PartialQuery>()
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
                val narr = findByTagAndProcessQuery(cleanQuery, "narr", " ")
                val date = getDates(cleanQuery)

                queries.add(PartialQuery(num, title, desc, narr, date))
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
                val titleQuery = MultiFieldQueryParser(fields, analyzer,  weights["title"]).parse(it)
                val boostedTitleQuery = BoostQuery(titleQuery, boosts["title"]!!)
                booleanQuery.add(boostedTitleQuery, BooleanClause.Occur.SHOULD)
            }
            query.desc?.let {
                val descQuery = MultiFieldQueryParser(fields, analyzer, weights["desc"]).parse(it)
                val boostedDescQuery = BoostQuery(descQuery, boosts["desc"]!!)
                booleanQuery.add(boostedDescQuery, BooleanClause.Occur.SHOULD)
            }
            query.narr?.let {
                val narrQuery = MultiFieldQueryParser(fields, analyzer, weights["narr"]).parse(it)
                val boostedNarrQuery = BoostQuery(narrQuery, boosts["narr"]!!)
                booleanQuery.add(boostedNarrQuery, BooleanClause.Occur.SHOULD)
            }
            query.date?.let {
                val dateQuery = MultiFieldQueryParser(fields, analyzer, weights["date"]).parse(it)
                val boostedDateQuery = BoostQuery(dateQuery, boosts["date"]!!)
                booleanQuery.add(boostedDateQuery, BooleanClause.Occur.SHOULD)
            }
            query.num?.let {
                processedQueries.add(QueryWithId(it, booleanQuery.build()))
            }
        }
//        println("${processedQueries.size} queries processed.")
        return processedQueries
    }    
            

    fun search(query: Query, isearcher : IndexSearcher): Array<ScoreDoc> {
        val hits = isearcher.search(query, 1000).scoreDocs

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
        val resultsFile = File("results/_output.txt")

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
//            println("Query $queryId searched")
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
                if (args.any { it.startsWith("-i") } ) {
                    println("Building index...")
                    ind = Indexer(qi.analyzer, qi.directory)
                    // if -iw flag is passed, save files to test file
                    launch(Dispatchers.Default) { ind!!.indexAll(1, args.contains("-iw")) }
                } else {
                    println("Using existing index.")
                }
            }
            qi.importQueries()
          
            if (args.any { it.startsWith("-i") } ) {
                println("Indexing took ${(System.currentTimeMillis() - startTime) / 1000}s")
            }

            // shutdown the indexer if it was initialized
            ind?.run {
                shutdown()
            }

            // if args contains flag starting in -o, run optimiser
            if (args.any { it.startsWith("-o")}) {
                Optimiser(qi).run {
                    if (args.contains("-os") || args.contains("-o")) {
                        optimiseSimilarities()
                        runTrecEval("optimisation/similarities", listOf("classic", "bm25", "lmd", "lmj"))
                    }
                    if (args.contains("-ow") || args.contains("-o")) {
                        optimiseWeights()
                        runTrecEval("optimisation/weights", listOf("title", "desc", "narr"))
                    }
                    if (args.contains("-ob") || args.contains("-o")) {
                        optimiseBoosts()
                        runTrecEval("optimisation/boosts")
                    }
                    if (args.contains("-ot") || args.contains("-o")) {
                        optimiseTokenizers()
                        runTrecEval("optimisation/tokenizers")
                    }
                    if (args.contains("-otf") || args.contains("-o")) {
                        optimiseTokenFilters()
                        runTrecEval("optimisation/token_filters")
                    }
                }
            } else {
                qi.run { runQueries(processQueries()) }

                val process = ProcessBuilder("./trec_eval-9.0.7/trec_eval", "qrels/qrels.assignment2.part1", "results/_output.txt").start()
                println(process.inputStream.bufferedReader().use(BufferedReader::readText))
            }
            qi.directory.close()
            exitProcess(0)
        }
    }
}
