import nltk
from nltk import corpus, sent_tokenize, word_tokenize
from nltk.stem.porter import PorterStemmer
from pandas import DataFrame
import sklearn.feature_extraction.text as ext


def cluster_error_logs(df: DataFrame) -> DataFrame:
    """Cluster error logs using unsupervised learning."""

    def log_tokenizer(text: str) -> list[str]:
        # Tokenize by sentence, then by word
        tokens = [word for sent in sent_tokenize(text) for word in word_tokenize(sent)]
        # Remove words that are not made of alpha characters
        tokens = [word.lower() for word in tokens if word.isalpha()]
        # Remove morphological affixes from words, leaving only the word stem.
        stemmer = PorterStemmer()
        stemmed_tokens = []
        for token in tokens:
            if token.isalpha() and token not in stopwords:
                stemmed_tokens.append(stemmer.stem(token))
        return stemmed_tokens

    def get_stop_words() -> list[str]:
        # Get stop words
        nltk.download("stopwords", quiet=True)
        stopwords = corpus.stopwords.words("english")
        # Add additional stop words that otherwise produce warnings
        stopwords.extend(["could", "might", "must", "need", "sha", "wo", "would"])
        return stopwords

    # Get input data
    stopwords = get_stop_words()

    # Learn the vocabulary dictionary and return document-term matrix.
    count_vectorizer = ext.CountVectorizer(
        tokenizer=log_tokenizer, stop_words=stopwords, token_pattern=None
    )
    doc_matrix = count_vectorizer.fit_transform(df["error"])
    # Learn the idf vector (global term weights), then Tf-idf-weighted document-term matrix
    tf_idf_transformer = ext.TfidfTransformer().fit_transform(doc_matrix)
    log_scores = tf_idf_transformer.toarray()

    per_log_score = []
    for row in log_scores:
        score = row.sum() / len(row.nonzero()[0]) if len(row.nonzero()[0]) > 0 else 0
        per_log_score.append(score)

    df["error_score"] = per_log_score
    df = df.groupby(["error_score"]).agg(
        {
            "error_score": "count",
            "task_instance_id": lambda x: list(set(x)),
            "task_id": lambda x: list(set(x)),
            "error": "first",
            "workflow_run_id": "first",
            "workflow_id": "first",
            "error_time": "first",
        }
    )
    df.rename(
        columns={
            "error": "sample_error",
            "error_score": "group_instance_count",
            "task_id": "task_ids",
            "error_time": "first_error_time",
            "task_instance_id": "task_instance_ids",
        },
        inplace=True,
    )
    df.sort_values(by=["group_instance_count"], ascending=False, inplace=True)
    return df
