from functools import cache

import pymorphy2
import nltk

lemmatizer = pymorphy2.MorphAnalyzer()


@cache
def normal_form_cached(word: str):
    return lemmatizer.parse(word)[0].normal_form


class TextLemmatizer:
    def lemmatize_text(self, text: str) -> str:
        tokens = nltk.word_tokenize(text)
        new_text = ''
        for word in tokens:
            norm_word = normal_form_cached(word.lower())
            new_text = new_text + ' ' + norm_word
        return new_text
