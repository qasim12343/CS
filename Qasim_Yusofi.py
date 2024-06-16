import pandas as pd
from ydata_profiling import ProfileReport

# question 1


def makeDashboard(user_file):
    df = pd.read_csv(user_file)
    pf = ProfileReport(df)
    pf.to_file('Qasim.html')


makeDashboard('wiki.csv')


# question 2
def q2(user_file):
    df = pd.read_csv(user_file)

    ls = df['Genre'].unique()
    print(ls)


q2('wiki.csv')
