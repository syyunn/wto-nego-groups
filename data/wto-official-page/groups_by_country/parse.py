from bs4 import BeautifulSoup
import pandas as pd

df_mmbr = pd.DataFrame()


pHtml = "./WTO _ Doha Development Agenda_ Negotiations, implementation and development - Groups in the negotiations.htm"
f = open(pHtml, "br")
html = f.read()
bs = BeautifulSoup(html)
tb = bs.find("table", {"id": "grouptable"})
tss = tb.findAll("tr")
urls = dict()

for ts in tss[1:]:
    tds = ts.findAll("td")
    ctn = tds[0].text
    a_s = tds[1].findAll("a")
    groups = []
    for a in a_s:
        group = a.text.strip()
        df_mmbr = df_mmbr.append({'group': group, "member": ctn}, ignore_index=True)

        href = a.attrs["href"]
        if group not in list(urls.keys()):
            urls[group] = href

        groups.append(group)
    # print(ctn, groups)

df_group_urls = pd.DataFrame(urls.items(), columns=['group', 'url'])

df_mmbr.to_csv("./parsed/wto_nego_membership.csv", sep=",", index=False)
df_group_urls.to_csv("./parsed/wto_nego_group_urls.csv", sep=",", index=False)

if __name__ == "__main__":
    pass
