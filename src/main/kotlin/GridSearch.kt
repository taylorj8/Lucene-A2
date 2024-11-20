import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import me.tongfei.progressbar.ProgressBar
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.*
import java.io.BufferedReader
import java.io.File


class GridSearch(val qi: QueryIndex, val queries: List<QueryIndex.QueryWithId>) {
    fun searchSimilarities() {
        val k1s = generateSequence(0.2f) { String.format("%.1f", it + 0.2f).toFloat() }.take(10).toList()
        val bs = generateSequence(0.1f) { String.format("%.1f", it + 0.1f).toFloat() }.take(10).toList()
        val mus = (100..1000 step 100).toList().map { it.toFloat() }
        val lambdas = generateSequence(0.1f) { String.format("%.1f", it + 0.1f).toFloat() }.take(9).toList()

        val totalIterations = 1L + k1s.size * bs.size + mus.size + lambdas.size
        ProgressBar("Searching for optimal similarity measure", totalIterations).use { progress ->
            runBlocking {
                launch(Dispatchers.Default) {
                    val classicSimilarity = ClassicSimilarity()
                    searchAndStoreResults("results/grid_search/ClassicSimilarity.txt", classicSimilarity)
                    progress.step()
                }

                for (k1 in k1s) {
                    for (b in bs) {
                        launch(Dispatchers.Default) {
                            val bm25Similarity = BM25Similarity(k1, b)
                            searchAndStoreResults("results/grid_search/BM25_k1=${k1}_b=$b.txt", bm25Similarity)
                            progress.step()
                        }
                    }
                }

                for (mu in mus) {
                    launch(Dispatchers.Default) {
                        val lmDirichletSimilarity = LMDirichletSimilarity(mu)
                        searchAndStoreResults("results/grid_search/LMDirichletSimilarity_mu=$mu.txt", lmDirichletSimilarity)
                        progress.step()
                    }
                }

                for (lambda in lambdas) {
                    launch(Dispatchers.Default) {
                        val lmJelinekMercerSimilarity = LMJelinekMercerSimilarity(lambda)
                        searchAndStoreResults("results/grid_search/LMJelinekMercerSimilarity_lambda=$lambda.txt", lmJelinekMercerSimilarity)
                        progress.step()
                    }
                }
            }

        }
    }

    private fun searchAndStoreResults(fileName: String, similarity: Similarity) {
        val resultsFile = File(fileName)
        if (!resultsFile.createNewFile()) {
            resultsFile.writeText("")  // Clear the file if it exists
        }

        val ireader = DirectoryReader.open(qi.directory)
        val isearcher = IndexSearcher(ireader).also { it.similarity = similarity }
        for (queryWithId in queries) {
            val hits = qi.search(queryWithId.query, isearcher)
            for ((j, hit) in hits.withIndex()) {
                val doc = isearcher.doc(hit.doc)
                val docId = doc["docId"]

                val simName = fileName.split("/").last().split(".").first()
                resultsFile.appendText("${queryWithId.num} Q0 $docId ${j + 1} ${hit.score} $simName\n")
//                println("Processed: ${queryWithId.num} Q0 $docId ${j + 1} ${hit.score} $simName")
            }
            // replace all whitespace before Q0 with a single space
//            resultsFile.writeText(resultsFile.readText().replace(Regex("\\s+Q0"), " Q0"))
        }
        ireader.close()
    }

    fun runTrecEval() {
        val resultsFolder = "results/grid_search" // Path to the folder containing result files
        val outputCsv = "output.csv"   // Output CSV file path

        // Create or initialize the CSV file with headers if it doesn't exist
        val csvFile = File(outputCsv).apply {
            createNewFile()
            writeText("Filename,MAP\n") // Add headers
        }

        // Iterate over all files in the results folder
        File(resultsFolder).walk().filter { it.isFile }.forEach { file ->
            try {
                // Run the ./trec_eval command on the file
                val process = ProcessBuilder("./trec_eval-9.0.7/trec_eval", "qrels/qrels.assignment2.part1", file.absolutePath)
                    .start()
                val output = process.inputStream.bufferedReader().use(BufferedReader::readText)

                // Extract the map value from the output
                val mapLine = output.lines().find { it.startsWith("map") }
                if (mapLine != null) {
                    val mapValue = mapLine.split("all")[1].trim()

                    // Append the filename and map value to the CSV file
                    csvFile.appendText("${file.name},$mapValue\n")
                } else {
                    println("No MAP value found for ${file.name}")
                }

            } catch (e: Exception) {
                println("Error processing file ${file.name}: ${e.message}")
            }
        }
    }
}