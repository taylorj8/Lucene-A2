# Information Retrieval and Web Search, Group project

# 1. Running the code 
Navigate to:
/IR_WS_Grp1/Lucene-A2

The command below will run the base pipeline using the existing index and automatically apply trec_eval to the search results, with the resulting data being written to the terminal:

java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar 

To rebuild the index run:
java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar -i

To rebuild the index and save readable samples of the indexed documents to the test folder run:

java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar -iw

To run the wordnet query expansion pipeline (this results in a lower MAP score) run:

java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar -s

To run the LLM query expansion pipeline (this results in a lower MAP score) run:

java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar -l

To run the weight optimisation process use the command below, note this process was originally implemented with Kotlin co-routines on a local device with far greater RAM availability then the AWS VM, running it on the AWS VM may take significant time:

java -jar /home/jsmulvany127/IR_WS_Grp1/Lucene-A2/target/search-engine-1.0.jar -o

# 2. src/main/kotlin

The code for the search pipeline is contained within src/main/kotlin.

Indexer contains code to process all documents and build an index, the index built will be saved in /index and a sample of the indexed documents are visible in /test.

QueryIndex contains code to process all queries, carry out search and evaluate the results it also contains the projects main function which will handle user flags and automatically carry out the desired functionality described section 1. Results are saved to /results.

LLMQuerySearch implements the LLM Query expansion pipeline. 

# 3. LLMQueryExpansion

Contains a jupyter notebook that use the geminin API to build LLM expanded queries. 