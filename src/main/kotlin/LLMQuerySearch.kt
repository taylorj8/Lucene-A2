import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.BM25Similarity
import org.apache.lucene.search.similarities.Similarity
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import java.io.File
import java.nio.file.Paths
import java.io.BufferedReader

class LLMQuerySearch {
    private var similarity: Similarity = BM25Similarity()
    private val directory: Directory = FSDirectory.open(Paths.get("index"))

    fun importQueries(filePath: String): List<String> {
        val file = File(filePath)
        if (!file.exists()) {
            println("File not found: $filePath")
            return emptyList()
        }
        val regex = Regex("^\\d+\\.\\s*") // Remove numbering
        return file.readLines()
            .map { regex.replace(it.trim(), "") }
            .filter { it.isNotEmpty() }
    }

    fun buildMultiFieldQueries(queries: List<String>): List<Query> {
        // Ensure you use the same analyzer as the indexer
        val analyzer: Analyzer = CustomAnalyzer.builder()
            .withTokenizer("standard")
            .addTokenFilter("lowercase")
            .addTokenFilter("stop")
            .addTokenFilter("porterstem")
            .build()
        val fields = arrayOf("text", "headline", "date") // Fields to search
        val parser = MultiFieldQueryParser(fields, analyzer)

        return queries.map { queryText ->
            try {
                parser.parse(queryText)
            } catch (e: Exception) {
                println("Failed to parse query: \"$queryText\" - ${e.message}")
                MatchAllDocsQuery() // Default to a query that matches all documents
            }
        }
    }

    fun search(query: Query, isearcher: IndexSearcher): Array<ScoreDoc> {
        val hits = isearcher.search(query, 1000).scoreDocs
        if (hits.isEmpty()) {
            println("No results found for query: \"$query\"")
        }
        return hits
    }

    fun runQueries(queryFilePath: String) {
        val queries = importQueries(queryFilePath)
        if (queries.isEmpty()) {
            println("No queries to process.")
            return
        }

        val parsedQueries = buildMultiFieldQueries(queries)
        //println("Parsed Queries: $parsedQueries")

        val simName = similarity::class.simpleName
        val resultsFile = File("results/${simName}_results_llm.txt")
        resultsFile.apply {
            if (!exists()) createNewFile()
            writeText("") // Clear previous results
        }

        val ireader = DirectoryReader.open(directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }

        for ((queryId, query) in parsedQueries.withIndex()) {
            val hits = search(query, isearcher)
            hits.forEachIndexed { rank, hit ->
                val doc = isearcher.doc(hit.doc)
                val docId = doc.get("docId") ?: "Unknown"
                resultsFile.appendText("${queryId + 401} Q0 $docId ${rank + 1} ${hit.score} $simName\n")
            }
        }
        println("Results saved to ${resultsFile.path}.")
        val process = ProcessBuilder("./trec_eval/trec_eval", "qrels/qrels.assignment2.part1", "results/BM25Similarity_results_llm.txt").start()
        println(process.inputStream.bufferedReader().use(BufferedReader::readText))
        ireader.close()
        
    }
}
