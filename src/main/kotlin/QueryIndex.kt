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


class QueryIndex {
    // Need to use the same analyzer and index directory throughout, so initialize them here
    val directory: Directory = FSDirectory.open(Paths.get("index"))
    var analyzer: Analyzer = CustomAnalyzer.builder()
        .withTokenizer("standard")
        .addTokenFilter("lowercase")
        .addTokenFilter("stop")
        .addTokenFilter("porterstem")
        .addTokenFilter("asciifolding")
        .build()

    var similarity: Similarity
    var weights: Map<String, Map<String, Float>>
    var boosts: Map<String, Float>
    lateinit var partialQueries: List<PartialQuery>
    init {
        this.similarity = BM25Similarity(0.6f, 0.7f)
        this.weights = mapOf(
            "title" to mapOf("headline" to 0.1f, "date" to 0.2f, "text" to 0.9f),
            "desc" to mapOf("headline" to 0.2f, "date" to 0.0f, "text" to 0.7f),
            "narr" to mapOf("headline" to 0.2f, "date" to 0.8f, "text" to 0.7f),
            "date" to mapOf("headline" to 0.2f, "date" to 1.0f, "text" to 0.2f)
        )
        this.boosts = mapOf("title" to 1f, "desc" to 0.4f, "narr" to 0.2f,  "noDate" to 0.0f, "date" to 0.4f)// date was 1
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

    private fun processNarr(narr: String): String {
        // split the narrative into sentences
        val sentences = narr.split(Regex("(?<=[.!?])\\s+"))
        // keep sentences unless they contain "not relevant"
        val relevantSentences = sentences.filter { !it.contains("not relevant", ignoreCase = true) }
        return relevantSentences.joinToString(" ")
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
    fun importQueries(expansion: Boolean = false) {
        val queries = ArrayList<PartialQuery>()
        val file = File("queries/topics")
        val qi = QueryIndex();
        if (file.isFile) {
            val content = file.readText()
            val querySeparator = Pattern.compile("(?<=<top>\\s)[\\s\\S]*?(?=</top>)").matcher(content)
            while (querySeparator.find()) {
                val rawQuery = querySeparator.group()
                var cleanQuery: String
                //if expansion flag set then use wordnet to perform synonym query expansion 
                if(expansion){
          
                    val wnFilePath = "wn/wn_s.pl"
                    val synonymMap = qi.loadSynonymsFromProlog(wnFilePath)
                    val expandedQuery = expandQueryWithPrologSynonyms(rawQuery, synonymMap)
                    cleanQuery = sanitizeQuery(expandedQuery)
                }else{ 
                    cleanQuery = sanitizeQuery(rawQuery)
                }
                
               
                //  print(cleanQuery);
                val num = findByTagAndProcessQuery(cleanQuery, "num", "title" )
                val title = findByTagAndProcessQuery(cleanQuery, "title", "desc")
                val desc = findByTagAndProcessQuery(cleanQuery, "desc", "narr")
                var narr = findByTagAndProcessQuery(cleanQuery, "narr", " ")
//                println("Before: $narr")
//                narr = narr?.let { processNarr(it) }
//                println("After: $narr")
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
                val boostedDateQuery = if (it == "null") {
                    BoostQuery(dateQuery, boosts["noDate"]!!)
                } else {
                    BoostQuery(dateQuery, boosts["date"]!!)
                }
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


    fun loadSynonymsFromProlog(filePath: String): Map<String, List<String>> {
        val synonymMap = mutableMapOf<String, MutableList<String>>()
      //  val regex = Regex("s\\((\\d+), \\d+, '(.*?)', [a-z], \\d+, \\d+\\)\\.")
        val regex = Regex("s\\((\\d+),\\d+,'(.*?)',n,\\d+,\\d+\\)\\.")

        File(filePath).useLines { lines ->
            for (line in lines) {
                val match = regex.find(line)
                if (match != null) {
                    val synsetId = match.groupValues[1]
                    val word = match.groupValues[2]

                    synonymMap.computeIfAbsent(synsetId) { mutableListOf() }.add(word)
                }
            }
        }
        return synonymMap
    }
    fun getSynonymsFromMap(word: String, synonymMap: Map<String, List<String>>): List<String> {
        val lowerCaseWord = word.lowercase()
        for ((_, words) in synonymMap) {
            if (lowerCaseWord in words.map { it.lowercase() }) {
                return words.filter { it.lowercase() != lowerCaseWord }  // Exclude original word
            }
        }
        return emptyList()
    }

    fun expandQueryWithPrologSynonyms(query: String, synonymMap: Map<String, List<String>>): String {
        val words = query.split("\\s+".toRegex())
       // println(words);
        val expandedWords = words.flatMap { word ->
          // println(word);
           // println(getSynonymsFromMap(word, synonymMap))
            listOf(word) + getSynonymsFromMap(word, synonymMap)

        }
       // println(expandedWords);
        return expandedWords.joinToString(" ")
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
            
            if (args.any { it.startsWith("-i") } ) {
                println("Indexing took ${(System.currentTimeMillis() - startTime) / 1000}s")
            }
            
            //if args conatins -l  then use llm query parsing pipeline  
            if (args.any { it.startsWith("-l") }){
                val qillm = LLMQuerySearch()
                qillm.runQueries("queries/expanded_queries.txt");
            //else use standard pipeline
            }else {
                //if args contains -s flag then use synonym expansion for queries
                if (args.any { it.startsWith("-s")}) {
                    qi.importQueries(expansion=true)
                }else{
                    qi.importQueries()
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
                        runTrecEval("optimisation/weights", listOf("title", "desc", "narr", "date"))
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
                }else{
                    qi.run { runQueries(processQueries()) }

                    val process = ProcessBuilder("./trec_eval/trec_eval", "qrels/qrels.assignment2.part1", "results/_output.txt").start()
                    println(process.inputStream.bufferedReader().use(BufferedReader::readText))
                }
                
                qi.directory.close()
                exitProcess(0)
    
            }

    }
}
}
