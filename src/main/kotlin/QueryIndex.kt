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
import org.apache.lucene.search.BoostQuery
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

    fun findByTagAndProcessQuery(doc: String, tag1: String, tag2: String): String? {
    
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

    fun getDates(text: String): String {
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


    fun importQueries(): List<QueryWithId> {
        val queries = ArrayList<QueryWithId>()
        val file = File("queries/topics")
        val qi = QueryIndex();
        if (file.isFile) {
            
            val content = file.readText()
            val querySeparator = Pattern.compile("(?<=<top>\\s)[\\s\\S]*?(?=</top>)").matcher(content)
            while (querySeparator.find()) {
                
                val rawQuery = querySeparator.group()
                val wnFilePath = "wn/wn_s.pl"
                val synonymMap = qi.loadSynonymsFromProlog(wnFilePath)
//println(synonymMap);
                val expandedQuery = expandQueryWithPrologSynonyms(rawQuery, synonymMap)
              //  println(rawQuery);
                println(expandedQuery);
                val cleanQuery = sanitizeQuery(expandedQuery)

              //  print(cleanQuery);
                val num = findByTagAndProcessQuery(cleanQuery, "num", "title" )
                val title = findByTagAndProcessQuery(cleanQuery, "title", "desc")
                val desc = findByTagAndProcessQuery(cleanQuery, "desc", "narr")
                val narr = findByTagAndProcessQuery(cleanQuery, "narr", " " )
                val date = getDates(cleanQuery)
    
                
               
                // Specify the fields and weights for the MultiFieldQueryParser
                val fields = arrayOf("headline", "date", "text")
                val fieldWeightsTitle = mapOf("headline" to 0.4f, "date" to 0.0f, "text" to 1f)
                val fieldWeightsDesc = mapOf("headline" to 0.4f, "date" to 0.0f, "text" to 1f)
                val fieldWeightsNarr = mapOf("headline" to 0.4f, "date" to 0.0f, "text" to 1f)
                var fieldWeightsDate = mapOf("headline" to 0.2f, "date" to 1f, "text" to 0.2f)
                if (date != "null") {
                    fieldWeightsDate = mapOf("headline" to 0.0f, "date" to 0f, "text" to 0.0f)
                }
                val booleanQuery = BooleanQuery.Builder()
                title?.let {
                    val titleQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsTitle).parse(it)
                    val boostedTitleQuery = BoostQuery(titleQuery, 1.0f)
                    booleanQuery.add(boostedTitleQuery, BooleanClause.Occur.SHOULD)
                }
                desc?.let {
                    val descQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsDesc).parse(it)
                    val boostedDescQuery = BoostQuery(descQuery, 1.0f)
                    booleanQuery.add(boostedDescQuery, BooleanClause.Occur.SHOULD)
                }
                narr?.let {
                    val narrQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsNarr).parse(it)
                    val boostedNarrQuery = BoostQuery(narrQuery, 1.0f)
                    booleanQuery.add(boostedNarrQuery, BooleanClause.Occur.SHOULD)
                }
                date?.let {
                    val dateQuery = MultiFieldQueryParser(fields, analyzer, fieldWeightsDate).parse(it)
                    val boostedDateQuery = BoostQuery(dateQuery, 1.0f)
                    booleanQuery.add(boostedDateQuery, BooleanClause.Occur.SHOULD)
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
        val resultsFile = File("results/${simName}_results.txt")

        val ireader = DirectoryReader.open(directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }

        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")  // Clear the file if it exists
        }
    
        for ((index, queryWithId) in queries.withIndex()) {
            val query = queryWithId.query
            val queryId = queryWithId.num
            // Use IndexSearcher to retrieve documents from the index based on BooleanQuery
            val hits = search(query, isearcher)
            println("Query ${queryId} searched")
            // Write hits to file compatible with trec_eval
            for ((j,hit) in hits.withIndex()) {
              
      
                val doc = isearcher.doc(hit.doc)
                val docId = doc.get("docId")  
                resultsFile.appendText("${queryId} Q0 $docId ${j+1} ${hit.score} $simName\n")
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

            
            // use existing index unless -i flag is passed
            if (args.isNotEmpty() && args[0] == "-i") {
                val ind = Indexer(qi.analyzer, qi.directory, qi.similarity)
                ind.indexLaTimes()
                ind.indexFt()
                ind.indexFBis()
                ind.indexFr94()
                ind.shutdown()
            } else {
//                val ind = Indexer(qi.analyzer, qi.directory, qi.similarity)
//                ind.indexLaTimes()
//                ind.indexFt()
//                ind.indexFBis()
//               // ind.shutdown()
//                ind.indexFr94()
//                ind.shutdown()
                println("Using existing index.")
            }



            val queries = qi.importQueries()

            qi.runQueries(queries);

             println("working on query expansion using LLM")
qillm.runQueries("queries/expanded_queries.txt");
            println("saved queries using LLM")
//
//            qi.directory.close()
            exitProcess(0)


    }}
}
