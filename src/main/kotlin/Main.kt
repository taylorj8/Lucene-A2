import org.apache.lucene.search.similarities.ClassicSimilarity


fun main() {
    print("test")
    val qi = QueryIndex()
    qi.buildIndex("/home/jamjar/tcd/ir-ws/assignment1/cran/cran.all.1400")
    val queries = qi.importQueries("/home/jamjar/tcd/ir-ws/assignment1/cran/cran.qry")
    qi.correctQrel("/home/jamjar/tcd/ir-ws/assignment1/cran/cranqrel")
    qi.runQueries(queries)

    // change similarity score and re-run queries
    qi.similarity = ClassicSimilarity()
    qi.buildIndex("/home/jamjar/tcd/ir-ws/assignment1/cran/cran.all.1400")
    qi.runQueries(queries)

    qi.shutdown()
}