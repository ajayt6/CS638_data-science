import pandas as pd

j = 0

def mergeRow(row_A,row_B):
    global j
    j = j + 1
    row_C = {}


    #1)Combine title based on length
    title = ""
    title_A = row_A.iloc[0]['Title']
    title_B = row_B.iloc[0]['Title']
    if len(title_A) > len(title_B):
        title = title_A
    else:
        title = title_B

    #2)Combine rating based on availability of data
    rating = ""
    rating_A = row_A.iloc[0]['Rating']
    rating_B = row_B.iloc[0]['Rating']
    if rating_A != "NR":
        rating = rating_A
    else:
        rating = rating_B

    #3)Combine releaseDate based on availability of data
    releaseDate = ""
    releaseDate_A = row_A.iloc[0]['ReleaseDate']
    releaseDate_B = row_B.iloc[0]['ReleaseDate']
    if releaseDate_A != "Dec 31,9999":
        releaseDate = releaseDate_A
    else:
        releaseDate = releaseDate_B

    #4)Combine writers based on set union -> to get all writer names . Optionally use a string similarity metric to avoid duplicates
    writers = ""
    writers_A = str(row_A.iloc[0]['Writers']).split(",")
    writers_B = str(row_B.iloc[0]['Writers']).split(",")

    writers_A_set = set()
    for writer in writers_A:
        writer = writer.strip()
        writers_A_set.add(writer)

    writers_B_set = set()
    for writer in writers_B:
        writer = writer.strip()
        writers_B_set.add(writer)

    #writers_A_set = set(writers_A)
    #writers_B_set = set(writers_B)
    writers_set = writers_A_set.union(writers_B_set)
    for writer in writers_set:
        if writer != "NOT_AVAILABLE" and writer != "nan":
            writers = writers + writer + ", "
    writers = writers[:len(writers)-2]

    #5)Combine directors based on set union -> to get all writer names . Optionally use a string similarity metric to avoid duplicates
    directors = ""
    directors_A = str(row_A.iloc[0]['Directors']).split(",")
    directors_B = str(row_B.iloc[0]['Directors']).split(",")

    directors_A_set = set()
    for director in directors_A:
        director = director.strip()
        directors_A_set.add(director)

    directors_B_set = set()
    for director in directors_B:
        director = director.strip()
        directors_B_set.add(director)

    #directors_A_set = set(directors_A)
    #directors_B_set = set(directors_B)
    directors_set = directors_A_set.union(directors_B_set)
    for director in directors_set:
        if director != "NOT_AVAILABLE" and director != "nan":
            directors = directors + director + ", "
    directors = directors[:len(directors) - 2]

    #6)Combine genre based on availability of data
    genre = ""
    genre_A = row_A.iloc[0]['Genre']
    genre_B = row_B.iloc[0]['Genre']
    if genre_A != "NOT_AVAILABLE":
        genre = genre_A
    else:
        genre = genre_B

    #7)Combine revenue based on availability of data
    revenue = ""
    revenue_A = (row_A.iloc[0]['Revenue'])
    revenue_B = (row_B.iloc[0]['Revenue'])
    if revenue_A != "0":
        revenue = revenue_A
    else:
        revenue = revenue_B

    #8)Combine runtime based on availability of data
    runtime = ""
    runtime_A = row_A.iloc[0]['Runtime']
    runtime_B = row_B.iloc[0]['Runtime']
    if runtime_A != "0":
        runtime = runtime_A
    else:
        runtime = runtime_B

    row_C['ID'] = j
    row_C['Title'] = title
    row_C['Rating'] = rating
    row_C['ReleaseDate'] = releaseDate
    row_C['Writers'] = writers
    row_C['Directors'] = directors
    row_C['Genre'] = genre
    row_C['Revenue'] = revenue
    row_C['Runtime'] = runtime
    #row_C['PartialURL'] = title

    return row_C

if __name__ == "__main__":
    #Get all data of table G to Pandas dataFrame
    path_G = ".\TableG.csv"
    path_A = ".\TableA.csv"
    path_B = ".\TableB.csv"
    path_E = ".\TableE.csv"

    df_G = pd.read_csv(path_G, encoding = "ISO-8859-1")
    df_A = pd.read_csv(path_A, encoding = "ISO-8859-1")
    df_B = pd.read_csv(path_B, encoding = "ISO-8859-1")

    #For rows with 'Match' == 1 , get corresponding rows from Table A and table B
    for i, value in enumerate(df_G['Match']):

        if value == 1:
            ltable_ID = df_G.loc(i)
            rtable_ID = df_G.loc(i)
            #print(ltable_ID['ltable_ID'])
            #print("At: " + str(i) + " Match is: " + str(value))
            #df.loc[df['column_name'] == some_value]
            pass

    df_E = pd.DataFrame(columns= df_A.columns)
    del df_E['PartialURL']
    #df_E.columns = df_A.columns

    for index, row in df_G.iterrows():
        if row['Match'] == 1:
            ltable_ID = row['ltable_ID']
            rtable_ID = row['rtable_ID']

            row_A = (df_A.loc[df_A['ID'] == ltable_ID])
            row_B = (df_B.loc[df_B['ID'] == rtable_ID])


            #Let each row go through a set of custom functions that takes care of merging each and every column and adding the row to a new dataFrame
            row_E = mergeRow(row_A,row_B)
            #print(row_E)
            df_E = df_E.append(pd.Series(row_E),ignore_index=True)

    #for index, row in df_E.iterrows():
        #print(row)
    df_E.to_csv(path_E, index=False)
#Save this final dataFrame
