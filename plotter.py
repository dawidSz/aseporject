import json
import matplotlib.pyplot as plt


def count_occurances(collectedData):
    names_occ, company_occ, city_occ = {}, {}, {}
    for record in collectedData:
        name = record[3]
        if name not in names_occ:
            names_occ[name] = 1
        else:
            names_occ[name] += 1

        city = record[2]
        if city not in city_occ:
            city_occ[city] = 1
        else:
            city_occ[city] += 1

        company = record[1]
        if company not in company_occ:
            company_occ[company] = 1
        else:
            company_occ[company] += 1

    return names_occ, company_occ, city_occ


def show_plot(data, title):
    plt.title(title)
    plt.bar(*zip(*data.items()))
    plt.xticks(rotation=90, fontsize=5)
    plt.show()


if __name__ == '__main__':
    DataFileName = 'dataMay-31-2017.json'

    with open(DataFileName, 'r') as DataFile:
        collectedData = json.load(DataFile)['data']

    names_occ, company_occ, city_occ = count_occurances(collectedData)
    show_plot(names_occ, 'Names')
    show_plot(company_occ, 'Companies')
    show_plot(city_occ, 'Cities')
