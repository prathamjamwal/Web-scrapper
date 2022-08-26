from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
import sqlite3
from tkinter import *
from tkinter import ttk
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

webs = list()


def callMain():
    cur.execute('''CREATE TABLE IF NOT EXISTS Pages
        (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
        error INTEGER, old_rank REAL, new_rank REAL)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Links
        (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

    # Check to see if we are already in progress...
    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    row = cur.fetchone()

    def startUrlJob(starturl):
        if (starturl.endswith('/')): starturl = starturl[:-1]
        web = starturl
        if (starturl.endswith('.htm') or starturl.endswith('.html')):
            pos = starturl.rfind('/')
            web = starturl[:pos]

        if (len(web) > 1):
            cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
            cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (starturl,))
            conn.commit()

    if row is not None:
        print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
    else:
        starturl = txturl
        startUrlJob(starturl)

    def printList():
        # Get the current webs
        cur.execute('''SELECT url FROM Webs''')

        for row in cur:
            webs.append(str(row[0]))

        print(webs)

    printList()


def callSec():
    many = 0
    while True:
        if (many < 1):
            sval = 10
            many = int(sval)
        many = many - 1

        cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
        try:
            row = cur.fetchone()
            # print row
            fromid = row[0]
            url = row[1]
        except:
            print('No unretrieved HTML pages found')
            many = 0
            break

        print(fromid, url, end=' ')

        # If we are retrieving this page, there should be no links from it
        cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
        try:
            document = urlopen(url, context=ctx)

            html = document.read()
            if document.getcode() != 200:
                print("Error on page: ", document.getcode())
                cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))

            if 'text/html' != document.info().get_content_type():
                print("Ignore non text/html page")
                cur.execute('DELETE FROM Pages WHERE url=?', (url,))
                conn.commit()
                continue

            print('(' + str(len(html)) + ')', end=' ')

            soup = BeautifulSoup(html, "html.parser")
        except KeyboardInterrupt:
            print('')
            print('Program interrupted by user...')
            break
        except:
            print("Unable to retrieve or parse page")
            cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
            conn.commit()
            continue

        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
        cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
        conn.commit()

        # Retrieve all of the anchor tags
        tags = soup('a')
        count = 0
        for tag in tags:
            href = tag.get('href', None)
            if (href is None): continue
            # Resolve relative references like href="/contact"
            up = urlparse(href)
            if (len(up.scheme) < 1):
                href = urljoin(url, href)
            ipos = href.find('#')
            if (ipos > 1): href = href[:ipos]
            if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')): continue
            if (href.endswith('/')): href = href[:-1]
            # print href
            if (len(href) < 1): continue

            # Check if the URL is in any of the webs
            found = False
            for web in webs:
                if (href.startswith(web)):
                    found = True
                    break
            if not found: continue

            cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (href,))
            count = count + 1
            conn.commit()

            cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href,))
            try:
                row = cur.fetchone()
                toid = row[0]
            except:
                print('Could not retrieve id')
                continue
            # print fromid, toid
            cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))

        print(count)

    cur.close()


# creating root object
root = Tk()

# defining size of window
root.geometry("870x550")

# setting up the title of window
root.title("WEB SCRAPPER")

# setting background color
root["bg"] = "blue"


# exit function
def qExit():
    root.destroy()


# labels for heading
lblInfo = Label(root, font=('arial', 30, 'bold'),
                text="  WEB SCRAPPING ",
                fg="white", bd=10, anchor='w')
lblInfo.grid(row=1, column=3)
lblInfo["bg"] = "grey"

# labels line 1
lblline1 = Label(root, font=('arial', 16, 'bold'),
                 text="==============================", bd=16, anchor="w")
lblline1.grid(row=3, column=3)
lblline1["bg"] = "grey"

# labels for the url entry
lblurl = Label(root, font=('arial', 16, 'bold'),
               text="Enter The Url", bd=16, anchor="w")
lblurl.grid(row=5, column=1)
lblurl["bg"] = "grey"

# Entry box for the url
txturl = Entry(root, font=('arial', 16, 'bold'), bd=10, insertwidth=4,
               bg="powder blue", justify='right')
txturl.grid(row=5, column=3)
txturl["bg"] = "grey"

# Submit button
btnsubmit1 = Button(root, padx=16, bd=10, fg="white", font=('arial', 10, 'bold'), width=7, text="SUBMIT", bg="grey",
                    command=callMain)
btnsubmit1.grid(row=6, column=3)
btnsubmit1["bg"] = "grey"

# labels line 2
lblline2 = Label(root, font=('arial', 16, 'bold'),
                  bd=16, anchor="w")
lblline2.grid(row=7, column=3)
lblline2["bg"] = "blue"





# Submit button
btnsubmit2 = Button(root, padx=16, bd=10, fg="white", font=('arial', 10, 'bold'), width=30, text="BEGIN SPIDERING ",
                    bg="grey", command=callSec)
btnsubmit2.grid(row=9, column=3)
btnsubmit2["bg"] = "grey"

# Exit button
btnExit = Button(root, padx=16, bd=10, fg="white", font=('arial', 10, 'bold'), width=7, text="Exit", bg="grey",
                 command=qExit)
btnExit.grid(row=10, column=4)
btnExit["bg"] = "grey"

# keeps window alive
root.mainloop()