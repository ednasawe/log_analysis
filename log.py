#!/usr/bin/env python3
import psycopg2


def connect(database_name):
    """connect to the news database.
    Args: database_name(str) with the name of the SQL database.
    Returns: A list of tuples containing the results of the database_name.
        A database connection
        And the connection cursor for that database
    Prints: error message if the connection fails
    """
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        c = db.cursor()
        c.execute(database_name)
        results = c.fetchall()
        db.close()
        return results
    except psycopg2.Error as e:
        print ("Database error occured\n", e)


def top_articles():
    """
    Return the top three most viewed articles,
    based on the number of article views.

    Args: none

    Returns: Sorted list of the title of the top
    three articles viewed.
    """

    query = """
        SELECT title, views
        from (
            select path, count (*) as views
            from log
            where path like '%article%'
            and log.status like '%OK%'
            group by path) as topthree, articles
        where topthree.path = '/article/' || articles.slug
        order by views desc
        limit 3
    """

    return connect(database_name)


def popular_authors():
    """
    Popular authors: Return all authors in order of popularity.
    args: none
    Returns: SOrted list of all the authors names with the most
    popular at the top.
    """

    query = """
        select name, sum(views) as author_views
        from (
            select the title, author, count (articles.title) as views
            from log, articles
            where log.path = concat('/article/', articles.slug)
            group by author) as allviews, authors
        where allviews.author = authors.id
        order by author_views desc
    """

    return connect(database_name)


def get_error_days():
    """Returns the days when more than 1% of requests led to
    HTTP requests errors.
    Args: none
    Returns: A list of the dates, the number of requests, the percentage
    of requests that led to the errors, the number of erros requests, and
    the dates with errors of 1%.
    """

    query = """
        select to_char(output.date, 'FMMonth FMDD, YYYY'),
            round((output.error*100.00/output.total), 1) as perc
        from (
            select response.date, errortable.error, response.total
            from (
                select date(time) as date, count (*) as error
                from log
                where status not like '%OK%'
                group by date) as errortable,
                (select date(time) as date, count (*) as total
                from log
                group by date) as response
            where errortable.date = response.date
            group by response.date, errortable.error, response.total) as output
        where (output.error*100.00/output.total) > 1.0
    """
    return connect(database_name)


def print_output():
    """Print connect database name results to a text file named output.txt"""

    print "Getting The Log Analysis"

    f = open("output.txt", "w")
    f.write("Logs Analysis:\n")

    dboutput = top_articles()
    f.write("\n1. Most Popular Articles:\n")
    for title, views in dboutput:
        entry = '{} - {} views\n'.format(title, views)
        f.write(entry)

    dboutput = popular_authors()
    f.write("\n\n2. Most Popular Authors:\n")
    for title, views in dboutput:
        entry = '{} - {} views\n'.format(title, views)
        f.write(entry)

    dboutput = get_error_days()
    f.write("\n\n3. More than 1% of requests led to errors on:\n")
    for date, perc in dboutput:
        entry = '{} - {}% requests\n'.format(date, perc)
        f.write(entry)

    f.close()

if __name__ == '__main__':
    print_output()
