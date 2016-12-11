import json
import re
from pymongo import MongoClient
from bson.code import Code


class DB(object):
    def initialization(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        data = json.load(open('test.json'))
        buyer_list = data['buyer']
        idBuyer = 1
        idBook = 1
        idJournal = 1
        idPurchase = 1
        for buyer in buyer_list:
            try:
                name = str(buyer['nameUser'])
                surname = str(buyer['surnameUser'])
                age = str(buyer['age'])
                db.Buyer.insert_one({"idBuyer": str(idBuyer), "nameBuyer": name, "surnameBuyer": surname, "age": age})
                idBuyer += 1
                print(name, surname, age)
            except IndexError:
                pass
            continue

        data = json.load(open('test.json'))
        journal_list = data['journal']
        for journal in journal_list:
            try:
                titleJournal = str(journal['titleJournal'])
                publisher = str(journal['publisher'])
                db.Journal.insert_one({"idJournal": str(idJournal),"titleJournal": titleJournal, "publisher": publisher})
                print(titleJournal, publisher)
                idJournal += 1
            except IndexError:
                pass
            continue

        data = json.load(open('test.json'))
        book_list = data['book']
        for book in book_list:
            try:
                titleBook = str(book['titleBook'])
                author = str(book['author'])
                publisherBook = str(book['publisher'])
                db.Book.insert_one({"idBook": str(idBook), "titleBook": titleBook, "author": author, "publisherBook":
                    publisherBook})
                print(titleBook, author)
                idBook += 1
            except IndexError:
                pass
            continue

        data = json.load(open('test.json'))
        purchase_list = data['purchase']

        for purchase in purchase_list:
            try:
                buyer = db.Buyer.find({"idBuyer": str(purchase['buyer'])})
                book = db.Book.find({"idBook": str(purchase['book'])})
                journal = db.Journal.find({"idJournal": str(purchase['titleJournal'])})
                price = int(purchase['price'])
                saleDate = str(purchase['saleDate'])
                db.Purchase.insert_one({"idPurchase": str(idPurchase),"buyDate": saleDate, "price": price,
                                        "journal": journal[0], "book": book[0], "buyer": buyer[0]})
                idPurchase += 1
            except IndexError:
                pass
            continue

    def getBuyer(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        buyers = db.Buyer.find()
        return buyers

    def getJournal(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        journal = db.Journal.find()
        return journal

    def getBooks(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        books = db.Book.find()
        return books

    def getPurchaseList(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        res = db.Purchase.find()
        return res

    def getPurchase(self, id):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        print(id)
        res = db.Purchase.find_one({"idPurchase": str(id)},{"idPurchase": 1, "buyer": 1, "book": 1,"journal": 1,
                                                        "price": 1, "buyDate": 1, "_id": 0})
        return res

    def savePurchase(self, buyDate, price, idBook, idJournal, idBuyer):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        idPurchase = db.Purchase.count()
        journal = db.Journal.find({"idJournal": idJournal})
        book = db.Book.find({"idBook": idBook})
        buyer = db.Buyer.find({"idBuyer": idBuyer})
        res = db.Purchase.insert_one({"idPurchase": str(idPurchase+1), "buyDate": buyDate, "price": price,
                                        "journal": journal[0], "book": book[0], "buyer": buyer[0]})

    def updatePurchase(self, idPurchase, buyDate, price, book, journal, buyer):
        print(idPurchase, buyDate, book, journal, buyer)
        client = MongoClient('localhost', 27017)
        db = client.mydb
        new_buyer = db.Buyer.find({"idBuyer": buyer})
        new_book = db.Book.find({"idBook": book})
        new_journal = db.Journal.find({"idJournal": journal})
        print (new_book[0])
        res = db.Purchase.update_one({"idPurchase": str(idPurchase)}, {'$set': {"buyer": new_buyer[0],
                                                                              "book": new_book[0],
                                                                                "journal": new_journal[0], "buyDate": buyDate,
                                                                            "price": price}})
        return res

    def removePurchase(self, id):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        print (id)
        db.Purchase.remove({"idPurchase": str(id)}, 1)

#logicalType
    def getPurclaseListByAge(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        res = db.Purchase.find({"buyer.age": {"$regex": u"True"}})
        return res

#date
    def getPurchaceListByDate(self, fromDate, toDate):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        res = db.Purchase.find({"buyDate": {"$gte": fromDate, "$lt": toDate}})
        return res

    def fullTextSearch(self, phrase):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        print(str(phrase))
        book = db.Book.find_one({"$text": {"$search": str(phrase), "$caseSensitive": True}})
        res = db.Purchase.find({"book.idBook": str(book['idBook'])})
        return res

    def aggregateFunction(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        res = db.Purchase.aggregate([{
            "$group":{
                "_id": "$buyer.idBuyer",
                "total": {"$sum": "$price"}
        }}])
        for i in res:
            print (i)

    def countOfBooks(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        print("Total amount of each book:")
        map = Code("function () {"
                   "  emit(this.book.titleBook, 1);"
                   "}")
        reduce = Code("function(key,values){"
                      "  var total = 0;"
                      "  for (i in values){"
                      "       total += values[i];"
                      "  }"
                      "  return total;"
                      "}")
        result = db.Purchase.map_reduce(map, reduce, "myresults")
        for doc in result.find():
            print(doc)

    def totalPrice(self):
        client = MongoClient('localhost', 27017)
        db = client.mydb
        print("Total amount of each buyer:")
        map = Code("function(){"
                   "  emit(this.buyer.idBuyer, this.price);"
                   "}")
        reduce = Code("function(key, value){"
                      "  return Array.sum(value);"
                      "}")
        result = db.Purchase.map_reduce(map, reduce, "result")
        for doc in result.find():
            print(doc)


