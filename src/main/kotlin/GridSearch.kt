import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import me.tongfei.progressbar.ProgressBar
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.*
import org.apache.lucene.store.FSDirectory
import java.io.BufferedReader
import java.io.File
import java.nio.file.Paths
import java.util.*
import kotlin.Comparator
import kotlin.collections.ArrayList
import kotlin.math.pow


class GridSearch(val qi: QueryIndex) {
    fun searchSimilarities() {
        val queries = qi.processQueries()

        val k1s = buildSequence(0.2f, 2f, 0.2f)
        val bs = buildSequence(0.1f, 1f, 0.1f)
        val mus = buildSequence(100f, 1000f, 100f)
        val lambdas = buildSequence(0.1f, 0.9f, 0.1f)

        val totalIterations = 1L + k1s.size * bs.size + mus.size + lambdas.size
        ProgressBar("Searching for optimal similarity measure", totalIterations).use { progress ->
            runBlocking {
                launch(Dispatchers.Default) {
                    qi.similarity = ClassicSimilarity()
                    searchAndStoreResults("grid_search/similarities/classic/ClassicSimilarity.txt", queries)
                    progress.step()
                }

                for (k1 in k1s) {
                    for (b in bs) {
                        launch(Dispatchers.Default) {
                            qi.similarity = BM25Similarity(k1, b)
                            searchAndStoreResults("grid_search/similarities/bm25/k1=${k1}_b=$b.txt", queries)
                            progress.step()
                        }
                    }
                }

                for (mu in mus) {
                    launch(Dispatchers.Default) {
                        qi.similarity = LMDirichletSimilarity(mu)
                        searchAndStoreResults("grid_search/similarities/lmd/mu=$mu.txt", queries)
                        progress.step()
                    }
                }

                for (lambda in lambdas) {
                    launch(Dispatchers.Default) {
                        qi.similarity = LMJelinekMercerSimilarity(lambda)
                        searchAndStoreResults(
                            "grid_search/lmj/lambda=$lambda.txt",
                            queries
                        )
                        progress.step()
                    }
                }
            }
        }
    }


    fun searchWeights() {
        val weights = buildSequence(0.0f, 1f, 0.1f)
        val totalIterations = weights.size.toFloat().pow(3).toLong() * 3
        ProgressBar("Searching for optimal weights", totalIterations).use { progress ->
            runBlocking {
                for (h in weights) {
                    for (t in weights) {
                        for (d in weights) {
                            launch(Dispatchers.Default) {
                                val testWeights = mapOf("headline" to h, "text" to t, "date" to d)
                                qi.weights = mapOf(
                                    "title" to testWeights,
                                    "desc" to testWeights,
                                    "narr" to testWeights
                                )

                                // test title
                                var queries = processPartialQueries(qi.partialQueries, "title")
                                searchAndStoreResults("grid_search/weights/title/$h-$t-$d.txt", queries)
                                progress.step()

                                // test desc
                                queries = processPartialQueries(qi.partialQueries, "desc")
                                searchAndStoreResults("grid_search/weights/desc/$h-$t-$d.txt", queries)
                                progress.step()

                                // test narr
                                queries = processPartialQueries(qi.partialQueries, "narr")
                                searchAndStoreResults("grid_search/weights/narr/$h-$t-$d.txt", queries)
                                progress.step()
                            }
                        }
                    }
                }
            }
        }
    }


    fun searchComparativeWeights() {
        val testWeights = buildSequence(0.0f, 1f, 0.1f)
        val titleWeights = mapOf("headline" to 0.0f, "date" to 0.2f, "text" to 0.5f)
        val descWeights = mapOf("headline" to 0.2f, "date" to 0.0f, "text" to 0.8f)
        val narrWeights = mapOf("headline" to 0.3f, "date" to 0.6f, "text" to 0.9f)
        val totalIterations = testWeights.size.toFloat().pow(3).toLong()
        ProgressBar("Searching for optimal weights", totalIterations).use { progress ->
            runBlocking {
                for (t in testWeights) {
                    for (d in testWeights) {
                        for (n in testWeights) {
                            launch(Dispatchers.Default) {
                                qi.run {
                                    weights = mapOf(
                                        "title" to mapOf(
                                            "headline" to titleWeights["headline"]!! * t,
                                            "date" to titleWeights["date"]!! * d,
                                            "text" to titleWeights["text"]!! * n
                                        ),
                                        "desc" to mapOf(
                                            "headline" to descWeights["headline"]!! * t,
                                            "date" to descWeights["date"]!! * d,
                                            "text" to descWeights["text"]!! * n
                                        ),
                                        "narr" to mapOf(
                                            "headline" to narrWeights["headline"]!! * t,
                                            "date" to narrWeights["date"]!! * d,
                                            "text" to narrWeights["text"]!! * n
                                        )
                                    )
                                    val queries = processQueries()
                                    searchAndStoreResults("grid_search/weights/comp/$t-$d-$n.txt", queries)
                                }
                                progress.step()
                            }
                        }
                    }
                }
            }
        }
    }


    fun searchAnalyzers() {
        val tokenizers = listOf("standard", "whitespace", "classic", "edgeNGram", "pathHierarchy")
        val tokenFilters = listOf("lowercase", "stop", "porterstem", "shingle", "wordDelimiter")

        val totalIterations = (tokenizers.size + tokenFilters.size).toLong()
        ProgressBar("Searching for optimal analyzers", totalIterations).use { progress ->
            for (tokenizer in tokenizers) {
                qi.analyzer = CustomAnalyzer.builder()
                    .withTokenizer(tokenizer)
                    .build()
                val ind = Indexer(qi.analyzer, FSDirectory.open(Paths.get("grid_search/test_index")))
                try {
                    ind.indexAll(4)
                    val queries = qi.processQueries()
                    searchAndStoreResults("grid_search/analyzers/tokenizer/$tokenizer.txt", queries)
                } catch (e: Exception) {
                    println("Tokenizer $tokenizer caused an error: ${e.message}")
                } finally {
                    ind.shutdown()
                }
                progress.step()
            }

            for (tokenFilter in tokenFilters) {
                qi.analyzer = CustomAnalyzer.builder()
                    .withTokenizer("standard")
                    .addTokenFilter(tokenFilter)
                    .build()
                val ind = Indexer(qi.analyzer, FSDirectory.open(Paths.get("grid_search/test_index")))
                try {
                    ind.indexAll(4)
                    val queries = qi.processQueries()
                    searchAndStoreResults("grid_search/analyzers/token-filter/$tokenFilter.txt", queries)
                } catch (e: Exception) {
                    println("Token Filter $tokenFilter caused an error: ${e.message}")
                } finally {
                    ind.shutdown()
                }
                progress.step()
            }
        }
    }


    private fun searchAndStoreResults(resultsFileName: String, queries: List<QueryIndex.QueryWithId>) {
        val resultsFile = File(resultsFileName)
        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")  // Clear the file if it exists
        }

        val ireader = DirectoryReader.open(qi.directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = qi.similarity }
        for (queryWithId in queries) {
            val hits = qi.search(queryWithId.query, isearcher)
            for ((j, hit) in hits.withIndex()) {
                val doc = isearcher.doc(hit.doc)
                val docId = doc["docId"]

                val simName = resultsFileName.split("/").last().split(".").first()
                resultsFile.appendText("${queryWithId.num} Q0 $docId ${j + 1} ${hit.score} $simName\n")
//                println("Processed: ${queryWithId.num} Q0 $docId ${j + 1} ${hit.score} $simName")
            }
        }
        ireader.close()
    }

    fun runTrecEval(basePath: String, folders: List<String> = listOf("")) {
        for (folder in folders) {
            // Create or initialize the CSV file with headers if it doesn't exist
            val csvFile = File("$basePath/$folder/_output.csv").apply {
                createNewFile()
                writeText("Filename,MAP\n") // Add headers
            }

            File("$basePath/$folder").walk().filter { it.isFile && it.name != "_output.csv" }.forEach { file ->
                // Keeps the MAP values in descending order
                val maps: TreeMap<Float, String> = TreeMap(Comparator.reverseOrder())
                try {
                    // Run the ./trec_eval command on the file
                    val process = ProcessBuilder("./trec_eval-9.0.7/trec_eval", "qrels/qrels.assignment2.part1", file.absolutePath)
                        .start()
                    val output = process.inputStream.bufferedReader().use(BufferedReader::readText)

                    // Extract the map value from the output
                    val mapLine = output.lines().find { it.startsWith("map") }
                    if (mapLine != null) {
                        val mapValue = mapLine.split("all")[1].trim().toFloat()
                        maps[mapValue] = file.name
                    } else {
                        println("No MAP value found for ${file.name}")
                    }

                } catch (e: Exception) {
                    println("Error processing file ${file.name}: ${e.message}")
                }
                maps.forEach { (map, filename) -> csvFile.appendText("$filename,$map\n") }
            }
            println("Results saved to ${csvFile.path}")
        }
    }


    private fun processPartialQueries(partialQueries: List<QueryIndex.PartialQuery>, field: String): List<QueryIndex.QueryWithId> {
        val processedQueries = ArrayList<QueryIndex.QueryWithId>()
        val fields = arrayOf("headline", "date", "text")
        for (partialQuery in partialQueries) {
            val query = when(field) {
                "title" -> {
                    partialQuery.title?.let {
                        MultiFieldQueryParser(fields, qi.analyzer, qi.weights["title"]).parse(it)
                    }
                }
                "desc" -> {
                    partialQuery.desc?.let {
                        MultiFieldQueryParser(fields, qi.analyzer, qi.weights["desc"]).parse(it)
                    }
                }
                "narr" -> {
                    partialQuery.narr?.let {
                        MultiFieldQueryParser(fields, qi.analyzer, qi.weights["narr"]).parse(it)
                    }
                }
                else -> null
            }
            if (query == null) continue
            partialQuery.num?.let {
                processedQueries.add(QueryIndex.QueryWithId(it, query))
            }
        }
        return processedQueries
    }
}


fun buildSequence(start: Float, end: Float, step: Float): List<Float> {
    return generateSequence(start) { String.format("%.1f", it + step).toFloat() }.takeWhile { it <= end }.toList()
}
