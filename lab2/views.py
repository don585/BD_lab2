from django.core.urlresolvers import reverse
from django.shortcuts import render, redirect
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage

from lab2 import database

# Create your views here.
def index(request):
    return render(request, 'adminpage.html',{'message': request.GET.get('message', None)})


def initializeDatabase(request):
    db = database.DB()
    db.initialization()
    #db.aggregateFunction()
    #db.countOfBooks()
    #db.totalPrice()
    return redirect(reverse('index')+ '?message=Database initialized')


def listView(request):
    db = database.DB()
    if 'Age' in request.GET:
        pur = db.getPurclaseListByAge()
    elif 'fromDate' in request.GET and 'toDate' in request.GET:
        pur = db.getPurchaceListByDate(str(request.GET['fromDate']), str(request.GET['toDate']))
    elif 'containsString' in request.GET:
        pur = db.fullTextSearch(str(request.GET['containsString']))
    else:
        pur = db.getPurchaseList()
    purchase = pur.fetchall()
    book = db.getBooks()
    paginator = Paginator(purchase, 2)
    page = request.GET.get('page')
    try:
        list = paginator.page(page)
    except PageNotAnInteger:
        list = paginator.page(1)
    except EmptyPage:
        list = paginator.page(paginator.num_pages)

    return render(request, 'listpage.html', {'list': rows, 'books': book})


def removePurchase(request, id):
    db = database.DB()
    db.removePurchase(id)
    return redirect(reverse('index') + '?message=Removed record')


def editPurchase(request, id):


    db = database.DB()
    if request.method == 'POST':
        print('asdsada')
        db.updatePurchase(id, request.POST['buyDate'], request.POST['price'], request.POST['idBook'],
                          request.POST['idJournal'], request.POST['idBuyer'])
        return redirect(reverse('index') + '?message=Changed purchase')
    journal = db.getJournal()
    book = db.getBooks()
    buyer = db.getBuyer()
    purchase = db.getPurchase(id)
    print (purchase)
    return render(request, 'editSale.html', {'buyers': buyer, 'books': book, 'journals': journal, 'purchase':
        purchase})



def addPurchase(request):
    db = database.DB()
    if request.method == 'GET':
        buyers = db.getBuyer()
        books = db.getBooks()
        journals = db.getJournal()
        return render(request,'addSale.html', {'buyers': buyers, 'books':books, 'journals':journals})
    elif request.method == 'POST':
        db.savePurchase(request.POST['buyDate'], request.POST['price'], request.POST['idBook'], request.POST[
            'idJournal'], request.POST['idBuyer'])
        return redirect(reverse('index') + '?message=Added Sale')
