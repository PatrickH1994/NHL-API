import pandas as pd
import os

"""
Functions are in alphabetical order
"""


def addGameID(df, game_id, home):
    """
    add column for the game_id
    """
    df['game_id'] = game_id
    df['home'] = home
    return df


def addPlayerName(insertedDf, lastName, firstName):
    """
    adds columns for the players name
    """
    df = insertedDf.copy()
    df['lastName'] = lastName
    df['firstName'] = firstName
    return df


def addToFileAndEmptyMemory(mainDf):
    file = getFile()
    if file is not None:
        mainDf = pd.concat([file, mainDf])
        print("The file has {} observations".format(len(file)))
        del file
    mainDf.to_csv("shiftsDataCollectedByPH.csv", index=False)
    print("New file added")


def addToMaindf(mainDf, playerDf, lastName, firstName, game_id, home):
    """
    Adds the tempDf to the mainDf
    """

    playerDf = dropEmptyColumns(playerDf)
    playerDf = addPlayerName(playerDf, lastName, firstName)
    playerDf = addGameID(playerDf, game_id, home)
    return pd.concat([mainDf, playerDf], axis=0, ignore_index=True)


def createMainDf():
    return pd.DataFrame(columns=['Shift #', 'Per', 'Start of ShiftElapsed / Game',
                                 'End of ShiftElapsed / Game', 'Duration', 'EventG=GoalP=Penalty', 'lastName',
                                 'firstName', 'game_id', 'home'])


def createTempDf(row):
    return pd.DataFrame(columns=row)


def dropEmptyColumns(df):
    """
    Dropping the two last columns as these columns have no values in them
    """
    return df.iloc[:, :-2]


def extractColumnsToAdd(row, colDict):
    """
    Turns the row to a dataframe with the same column names as the main dataframe
    """
    addDf = pd.DataFrame(data=[row])
    addDf.rename(columns=colDict, inplace=True)
    addDf.reset_index(inplace=True, drop=True)
    return addDf


def getFile():
    try:
        return pd.read_csv("shiftsDataCollectedByPH.csv")
    except:
        return None


def getPlayerName(previousRowIndex, table):
    """
    Retrieves the previous row, in which all the cells are the players name.
    After that it removes the comma from the last name and returns both the last and the first name.
    """
    name = ''.join(i for i in table.iloc[previousRowIndex].max() if not i.isdigit())
    lastName, firstName = name[1:].split(",")  # [1:] is used to get rid of the first space before the name
    return lastName, firstName


def getTable(season, game, homeOrVisitor):
    """
    Gets the table from NHL's website
    """
    season = str(season)
    game = str(game)
    url = "http://www.nhl.com/scores/htmlreports/" + str(season) + "/T" + homeOrVisitor + str(game) + ".HTM"
    return pd.io.html.read_html(url)[9]  # It's always the 9 nth table


def isNewPlayer(row):
    return row[0] == "Shift #"


def isSamePlayer(row):
    return len(row[2].split("/")) == 2 and len(row[3].split("/")) == 2


print(os.getcwd())
"""
I use datasets that are originally collected by Martin Ellis https://www.kaggle.com/martinellis/nhl-game-data.
The code is collects only the shifts data for the 2014-15 season, but it can be used for other seasons as well.
"""
# Prepare games data
games = pd.read_csv("game.csv")[
    ['game_id', 'season']].drop_duplicates()  # I use the original dataset, which has duplicates
games = games[games.season == 20142015]  # I only use games in the 2014-15 season

# Prepare player data
players = pd.read_csv("player_info.csv")[['player_id', 'firstName', 'lastName']]
players['firstName'] = players.firstName.apply(lambda x: x.lower())
players['lastName'] = players.lastName.apply(lambda x: x.lower())

mainDf = createMainDf()

gamesLeft = len(games)

missingGames_lst = []
n = 0
notFirst = False
for j, game in games.iterrows():  # Where the code was interrupted last time +++++
    n += 1
    if n % 100 == 0 and notFirst:
        addToFileAndEmptyMemory(mainDf)
        del mainDf
        mainDf = createMainDf()

    notFirst = True

    for homeOrVisitor in ['H', 'V']:  # There's separate files for home and visitor games

        try:
            table = getTable(game.season, str(game.game_id)[4:], homeOrVisitor)
        except:  # if the game does not have an page
            missingGames_lst.append(game.game_id)

        IsFirst = True
        for i, row in table.iterrows():  # Row is a row from a table in the shifts pdf
            try:  # Try block is used for cases where there is an empty row
                if isNewPlayer(row):
                    if not IsFirst:  # Adds the previous players' data
                        mainDf = addToMaindf(mainDf, tempDf, lastName, firstName, game.game_id, homeOrVisitor)
                        del tempDf
                    # Creates the "base" for tempDf to which I later add columns to
                    lastName, firstName = getPlayerName(i - 1, table)  # The name will be on the previous row
                    tempDf = createTempDf(row)
                    colDict = {i: tempDf.columns[i] for i in range(8)}

                    # The code jumps here when the row has playtime information
                elif isSamePlayer(row):
                    addDf = extractColumnsToAdd(row, colDict)
                    tempDf = pd.concat([tempDf, addDf], axis=0, ignore_index=True)
                    IsFirst = False
            except: continue

        mainDf = addToMaindf(mainDf, tempDf, lastName, firstName, game.game_id, homeOrVisitor)  # Adds the last tempDf
        del tempDf

file = getFile()
if file is not None:
    mainDf = pd.concat([file, mainDf])
mainDf.to_csv("shiftsDataCollectedByPH.csv", index=False)
