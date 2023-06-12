import pymorphy2
import nltk


class TextLemmatizer:
    def __init__(self):
        self.lemmatizer = pymorphy2.MorphAnalyzer()

    def lemmatize_text(self, text: str) -> str:
        tokens = nltk.word_tokenize(text)
        new_text = ''
        for word in tokens:
            # с помощью лемматайзера получаем основную форму
            word = self.lemmatizer.parse(word)
            # добавляем полученную лемму в переменную с преобразованным текстом
            new_text = new_text + ' ' + word[0].normal_form
            # возвращаем преобразованный текст
        return new_text
