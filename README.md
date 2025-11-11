# data-chat
A demo of a chatbot that can have a conversation with an arbitrary set of files by loading the data into structured and vectorized unstructured tables on Firebolt.

The general idea is that you have an S3 bucket with a set of files. CSVs, JSON, PARQUET, etc and a set of PDF documents. 
The system loads all the data into a database and it allows the user to have a conversation with the data.


Some design elements:

Source structured data files must contain column names.
Name of the files is significant and used for table names.
PDF titles define a knowledge domain (a different table) and content is chunked into the table with embedding index.


This chatbot uses 
- LangChain to process unstructured data. 
- Firebolt as a database for both analytics on structured data and vector search for semantic search of content.
- It responds to users questions based solely on the structured and un-structured data the user has access to.

