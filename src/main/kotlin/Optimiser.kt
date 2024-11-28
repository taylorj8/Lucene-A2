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


class Optimiser(val qi: QueryIndex) {
    fun searchSimilarities() {
        clearDirectory("optimisation/similarities")
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
                    searchAndStoreResults("optimisation/similarities/classic/ClassicSimilarity.txt", queries)
                    progress.step()
                }

                for (k1 in k1s) {
                    for (b in bs) {
                        launch(Dispatchers.Default) {
                            qi.similarity = BM25Similarity(k1, b)
                            searchAndStoreResults("optimisation/similarities/bm25/k1=${k1}_b=$b.txt", queries)
                            progress.step()
                        }
                    }
                }

                for (mu in mus) {
                    launch(Dispatchers.Default) {
                        qi.similarity = LMDirichletSimilarity(mu)
                        searchAndStoreResults("optimisation/similarities/lmd/mu=$mu.txt", queries)
                        progress.step()
                    }
                }

                for (lambda in lambdas) {
                    launch(Dispatchers.Default) {
                        qi.similarity = LMJelinekMercerSimilarity(lambda)
                        searchAndStoreResults("optimisation/similarities/lmj/lambda=$lambda.txt", queries)
                        progress.step()
                    }
                }
            }
        }
    }


    fun searchWeights() {
        clearDirectory("optimisation/weights")
        val weights = buildSequence(0.0f, 1f, 0.1f)
        val totalIterations = weights.size.toFloat().pow(3).toLong() * 3
        ProgressBar("Searching for optimal weights", totalIterations).use { progress ->
            runBlocking {
                for (h in weights) {
                    for (t in weights) {
                        for (d in weights) {
                            launch(Dispatchers.Default) {
                                val testWeights = mapOf("headline" to h, "date" to d, "text" to t)
                                qi.weights = mapOf(
                                    "title" to testWeights,
                                    "desc" to testWeights,
                                    "narr" to testWeights
                                )

                                // test title
                                var queries = processPartialQueries(qi.partialQueries, "title")
                                searchAndStoreResults("optimisation/weights/title/$h-$d-$t.txt", queries)
                                progress.step()

                                // test desc
                                queries = processPartialQueries(qi.partialQueries, "desc")
                                searchAndStoreResults("optimisation/weights/desc/$h-$d-$t.txt", queries)
                                progress.step()

                                // test narr
                                queries = processPartialQueries(qi.partialQueries, "narr")
                                searchAndStoreResults("optimisation/weights/narr/$h-$d-$t.txt", queries)
                                progress.step()
                            }
                        }
                    }
                }
            }
        }
    }


    fun searchBoosts() {
        clearDirectory("optimisation/boosts")
        val testWeights = buildSequence(0.0f, 1f, 0.2f)

        val totalIterations = testWeights.size.toFloat().pow(3).toLong()
        ProgressBar("Searching for optimal comparative weights", totalIterations).use { progress ->
            runBlocking {
                for (t in testWeights) {
                    for (d in testWeights) {
                        for (n in testWeights) {
                            launch(Dispatchers.Default) {
                                qi.run {
                                    boosts = mapOf("title" to t, "desc" to d, "narr" to n)
                                    val queries = processQueries()
                                    searchAndStoreResults("optimisation/boosts/$t-$d-$n.txt", queries)
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
        clearDirectory("optimisation/analyzers")
        val tokenizers = listOf("standard", "whitespace", "classic", "edgeNGram", "pathHierarchy")
        val tokenFilters = listOf("lowercase", "stop", "porterstem", "shingle", "wordDelimiter")

        val totalIterations = (tokenizers.size + tokenFilters.size).toLong()
        ProgressBar("Searching for optimal analyzers", totalIterations).use { progress ->
            for (tokenizer in tokenizers) {
                qi.analyzer = CustomAnalyzer.builder()
                    .withTokenizer(tokenizer)
                    .build()
                val ind = Indexer(qi.analyzer, FSDirectory.open(Paths.get("optimisation/test_index")))
                try {
                    ind.indexAll()
                    val queries = qi.processQueries()
                    searchAndStoreResults("optimisation/analyzers/tokenizer/$tokenizer.txt", queries)
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
                val ind = Indexer(qi.analyzer, FSDirectory.open(Paths.get("optimisation/test_index")))
                try {
                    ind.indexAll()
                    val queries = qi.processQueries()
                    searchAndStoreResults("optimisation/analyzers/token-filter/$tokenFilter.txt", queries)
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

private fun clearDirectory(path: String) {
    File(path).listFiles()?.forEach {
        if (it.isDirectory) clearDirectory(it.path)
        else if (it.isFile) it.delete()
    }
}

fun buildSequence(start: Float, end: Float, step: Float): List<Float> {
    return generateSequence(start) { String.format("%.1f", it + step).toFloat() }.takeWhile { it <= end }.toList()
}
