# Bussien myöhästymisanalyysin koontihavainnot

Tämä raportti kokoaa yhteen tämän repositorion uusimmat generoidut bussien
myöhästymisanalyysin tuotokset. Ensisijainen lähde on pandas/DuckDB-raportti
[`reports/generated/overall-results.md`](reports/generated/overall-results.md),
joka on generoitu `2026-10-19T08:07:13+00:00` uudelleen rakennetusta
välimuistista. Polars-raportti ja julkisen sivuston data rakennettiin samasta
tietokannasta, ja ne vastaavat sitä taulukko taulukolta (ks.
[Pandas/DuckDB ja Polars](#pandasduckdb-ja-polars)).

Tämä versio korvaa toukokuun 2026 havainnot analyysimenetelmän tarkastuksen
jälkeen. Osio [Mitä lokakuun 2026 tarkastus muutti](#mitä-lokakuun-2026-tarkastus-muutti)
luettelee korjaukset ja niiden vaikutuksen päälukuihin.

Luvut on kirjoitettu samassa muodossa kuin generoiduissa taulukoissa
(desimaalipiste, tuhaterotin pilkku), jotta ne on helppo tarkistaa lähteistä.

## Tiivistelmä

- Aineisto kattaa ajanjakson `2026-04-23T08:05:22Z`–`2026-05-23T13:24:20Z`:
  `10,430,580` analyysiriviä `10,532,270` raakamuotoisesta ajoneuvohavainnosta.
- Laatusuodatuksen jälkeen matka–pysäkki-aineistossa on `3,729,540` luokkaa
  ("pysäkkiohitusta"). Ne edustavat `8,582,662` raakakyselyä, `102,868` vuoroa ja
  `140` linjaa `31` päivän ajalta.
- Konservatiivinen laatusuodatus sulkee pois `1,847,918` riviä eli `17.72%`
  analyysiriveistä. Suurin osa noususta aiemmasta `5.69%`:sta johtuu uudesta
  poissulusta: `1,559,099` riviä (`14.95%`) on busseja, jotka odottavat
  lähtöpysäkillä ennen lähtöä. Föli raportoi näille paikkamerkkiviiveen 0.
- Koko verkossa `65.41%` pysäkkiohituksista on aikataulussa (1 minuutti
  etuajassa – 3 minuuttia myöhässä), `3.80%` on yli 5 minuuttia myöhässä ja
  `7.83%` yli 3 minuuttia etuajassa. Mediaaniviive on `0.02` minuuttia ja p90
  `2.88` minuuttia.
- Säännöllisistä linjoista (vähintään 20 vuoroa päivässä) myöhästyminen on
  yleisintä linjoilla `24` (`18.76%` pysäkkiohituksista yli 5 minuuttia
  myöhässä), `21` (`16.86%`) ja `220` (`10.07%`). Linjoilla `612`, `615` ja `614`
  on korkeimmat p90-viiveet. Kukin ajaa kuitenkin vain 2–4 vuoroa päivässä, joten
  niiden tulokset perustuvat 37–74 vuoroon.
- Etuajassa kulkeminen on erillinen ongelma. Säännöllisistä linjoista se on
  yleisintä linjalla `K3` (`28.40%` pysäkkiohituksista yli 3 minuuttia etuajassa),
  `801` (`19.57%`) ja `28` (`19.27%`). Äärimmäisimmät linjat ovat harvoin
  kulkevia palvelulinjoja: `P6` (`59.84%`), `75` (`46.30%`) ja `L2` (`44.14%`),
  kullakin 36–74 vuoroa.
- Arkisin myöhästymisten huippu on klo `16:00` (`8.73%` yli 5 minuuttia
  myöhässä) ja `15:00` (`8.40%`). Viikonloppuisin huippu on klo `15:00`
  (`6.44%`).
- Arkipäivän päiväliikenteeseen (09:00–15:00) verrattuna ruuhka lisää
  myöhästymisiä eniten linjoilla `721` (+`32.13` prosenttiyksikköä, 95 %:n
  luottamusväli `21.05`–`42.33`), `722S` (+`25.33`) ja `614` (+`24.38`). Aiempien
  ruuhkakärkien `612` ja `615` vaikutusta ei voi mitata näin, koska niillä ei ole
  arkipäivän päiväliikennettä.
- Selvin häiriösignaali on reittitason poikkeusreittitiedote: `+2.16`
  prosenttiyksikköä enemmän yli 5 minuuttia myöhässä olevia pysäkkiohituksia
  (95 %:n luottamusväli `1.75`–`2.59`) `370,654` pysäkkiohituksessa. Yksittäiset
  perutut lähdöt (yhteensä `74`) lasketaan nyt erikseen. Aiemmin jokainen niistä
  merkitsi koko linjan häiriötilaan aineiston loppuun asti.
- Myöhästymisten kannalta huonoin päivä oli vappupäivä `2026-05-01` (`8.66%`
  yli 5 minuuttia myöhässä), ja seuraavaksi huonoin vappuaatto `2026-04-30`
  (`6.63%`). Keräyskatkot 9.–10.5. ja 13.–14.5. jättävät neljä päivää alle 95 %:n
  keräyskattavuuden.

## Rajaus ja menetelmä

| Kohta | Arvo |
| --- | --- |
| Tietokanta | `data/foli.db` |
| Generoitu raportti | `reports/generated/overall-results.md` |
| Välimuistin manifesti | `outputs/report-cache/manifest.json` |
| Raportti generoitu | `2026-10-19T08:07:13+00:00` |
| Raakamuotoiset ajoneuvohavainnot | `10,532,270` |
| Analyysirivit | `10,430,580` |
| Välimuistin matka–pysäkki-luokat | `3,729,540` |
| Luokkien edustamat raakakyselyt | `8,582,662` |
| Eri vuorot | `102,868` |
| Edustetut linjat | `140` |
| Edustavan luokka-aineiston aikaväli | `2026-04-23T09:45:00Z`–`2026-05-23T13:33:57Z` |
| Laatutila | `conservative` |
| Luokittelutila | `trip-stop` |
| Aikavyöhyke | `Europe/Helsinki` |
| Ryhmien vähimmäiskoko | `30` luokkaa ja `10` eri vuoroa |
| Aikataulussa-raja | 1 minuutti etuajassa – 3 minuuttia myöhässä |
| Ruuhkaikkunat | `07:00-09:00`, `15:00-18:00` paikallista aikaa arkipäivisin |
| Vertailujakso | `09:00-15:00` paikallista aikaa arkipäivisin |
| Supistetun liikenteen pyhäpäivät | `2026-05-01`, `2026-05-14` |

Analyysi käyttää SIRI-ajoneuvoseurannan viivearvoja. Ne kuvaavat ajoneuvon
arvioitua tilaa seuraavaa pysäkkiä lähestyttäessä, eivät mitattuja saapumis- tai
lähtöaikoja. Raakarivit ovat toistuvia 30 sekunnin kyselyjä, joten
oletusraportti niputtaa ne matka–pysäkki-luokiksi: yksi arvo vuoroa ja seuraavaa
pysäkkiä kohden.

Lukujen tulkinnan kannalta tärkeät menetelmän yksityiskohdat:

- **Otoskoon määräävät vuorot, eivät luokat.** Yksi vuoro tuottaa noin 70
  toisiinsa korreloivaa luokkaa. Jokainen ryhmitelty taulukko vaatii vähintään
  `10` eri vuoroa sekä `30` luokkaa. 95 %:n luottamusvälit lasketaan
  bootstrap-menetelmällä, joka poimii kokonaisia vuoroja (300 toistoa, kiinteä
  siemen).
- **Pyhäpäivät lasketaan viikonlopuksi.** Maanantai–perjantai-päivä, jolla on
  alle 60 % tavallisista aktiivisista GTFS-palveluista, käsitellään
  pyhäpäivänä. Tällä jaksolla ne ovat vappupäivä (`2026-05-01`) ja
  helatorstai (`2026-05-14`), jotka aiemmin laskettiin tavallisiksi arkipäiviksi.
- **Osuudet, ei pelkkiä persentiilejä.** Järjestykset perustuvat nyt yli 5
  minuuttia myöhässä (`pct_over_5_min_late`) tai yli 3 minuuttia etuajassa
  (`pct_over_3_min_early`) olevien pysäkkiohitusten osuuteen. p90-viive
  raportoidaan edelleen luottamusvälin kanssa.

## Datan laatuun liittyvät havainnot

Oletuksena käytettävä konservatiivinen suodatin poistaa:

- epäuskottavat viiveet;
- vanhentuneet havainnot;
- ennen matkaa ja matkan jälkeen tehdyt havainnot;
- kaksi paikkamerkkinollan lajia:
  - odotus lähtöpysäkillä: seuraava pysäkki on vuoron lähtöpysäkki, ja havainto
    on tehty ennen aikataulun mukaista lähtöä;
  - vuorot ilman reaaliaikadataa: vuoron jokainen pysäkki raportoi tasan 0.

Pysäkkikutsun ristiriita ja jäljelle jäävät paikkamerkkinollat merkitään
lipulla, mutta niitä ei poisteta.

| Laatutarkistus | Rivit | Osuus |
| --- | ---: | ---: |
| Analyysirivit | 10,430,580 | 100.00% |
| Epäuskottava viive | 6,637 | 0.06% |
| Vanhentunut havainto | 149,992 | 1.44% |
| Ennen matkaa tehty havainto | 343,245 | 3.29% |
| Matkan jälkeen tehty havainto | 201,581 | 1.93% |
| Odotus lähtöpysäkillä (uusi) | 1,559,099 | 14.95% |
| Vuoro ilman reaaliaikadataa (uusi) | 31,177 | 0.30% |
| Pysäkkikutsun ristiriita (vain lippu) | 320,371 | 3.07% |
| Paikkamerkkinolla (vain lippu, uusi) | 664,631 | 6.37% |
| Konservatiivisen oletuksen poissulkemat | 1,847,918 | 17.72% |
| Konservatiivinen, kun myös pysäkkikutsun ristiriita poistetaan | 2,024,162 | 19.41% |

Odotus lähtöpysäkillä on suurin poissulku. Lähtöpysäkillä odottavaa bussia
kysytään 30 sekunnin välein, ja Föli raportoi useimmille näistä riveistä
viiveen tasan 0. Ennen korjausta hyväksytyissä riveissä tasan nolla esiintyi
noin 42 kertaa niin usein kuin kukin viereinen sekunti (1–5 sekuntia etu- tai
jälkiajassa). Korjauksen jälkeen suhde on noin 15. Rivien poistaminen vähentää
aineistosta vain `17,230` luokkaa, koska odottavan vuoron kyselyt niputtuvat
enimmäkseen yhteen luokkaan. Ne eivät kuitenkaan enää vedä päätepysäkkejä ja
koko verkon mediaania kohti täydellistä nollaa.

Pienempi nollapiikki jää jäljelle. Hyväksytyistä `477,453` tasan nollan
rivistä `199,285` poikkeaa seuraavan pysäkkikutsun ajoista yli minuutin.
Rivitason herkkyystarkastelu rajaa niiden vaikutuksen:

- Aikataulussa olevien osuus laskee `63.14%`:sta `62.27%`:iin (jos rivit
  poistetaan) tai `61.13%`:iin (jos ne korvataan pysäkkikutsun viiveellä).
- Yli 3 minuuttia etuajassa olevien osuus nousee `8.55%`:sta `9.33%`:iin, jos
  ne korvataan.
- Yli 5 minuuttia myöhässä olevien osuus tuskin muuttuu (`5.80%` → `5.86%`).

Aikataulussa olevien osuudet voivat siis olla noin 2 prosenttiyksikköä liian
optimistisia.

### Puutteellisen datan linjat

Poissuljettujen osuus kuvaa nyt pääosin sitä, kuinka kauan bussit odottavat
lähtöpysäkillä, joten se ei enää ole puhdas virhemittari. Linja merkitään
heikkolaatuiseksi, jos yli 20 % sen riveistä on datavirheitä (epäuskottavat,
vanhentuneet, matkan jälkeiset tai reaaliaikadatattomat rivit), tai jos alle
80 % sen aikataulun mukaisista vuoroista näkyy täyden keräyskattavuuden päivinä.

`19` linjaa on merkitty. Ne säilytetään aineistossa, mutta ne jätetään pois
säännöllisten linjojen vertailuista ja generoiduista nostoista.

| Linja | Rivit | Poissuljettujen osuus | Datavirheiden osuus |
| --- | ---: | ---: | ---: |
| `N14` | 5,035 | 72.41% | 48.90% |
| `L13` | 6,377 | 62.13% | 47.81% |
| `N10` | 16,349 | 47.51% | 31.97% |
| `P4` | 2,611 | 49.48% | 31.52% |
| `N6` | 13,832 | 63.01% | 26.53% |
| `79A` | 6,103 | 70.85% | 25.97% |
| `711` | 12,774 | 67.37% | 23.16% |
| `67` | 12,071 | 50.32% | 22.14% |

Muut merkityt linjat (`203`, `2Y`, `76`, `79`, `82`, `K1`, `K2`, `K4`, `K6`, `L8`,
`L9`) on merkitty matalan vuorokattavuuden, ei datavirheiden, vuoksi.
Äärimmäisimmin etuajassa kulkevalla linjalla `P6` poissuljettujen osuus on
korkea (`47.15%`), mutta datavirheitä on vain `7.46%` ja vuorokattavuus on
täysi, joten sitä ei enää merkitä. Sen poissuljetut rivit ovat enimmäkseen
odotusta lähtöpysäkillä.

Täydet taulukot:
[`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
[`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv),
[`line_quality.csv`](outputs/report-cache/line_quality.csv).

## Myöhässä kulkevien linjojen havainnot

Generoitu raportti järjestää myöhästymiset edelleen p90-viiveen mukaan. Tämä
nostaa kärkeen linjoja, jotka ajavat vain 2–4 vuoroa päivässä, joten alla oleva
taulukko näyttää säännölliset linjat (vähintään 20 vuoroa päivässä, ei
laatumerkintää). Välit ovat vuoroittain poimittuja 95 %:n luottamusvälejä.

| Linja | Vuoroja päivässä | Vuorot | Aikataulussa | >5 min myöhässä (95 %:n LV) | p90-viive, min (95 %:n LV) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `24` | 33.4 | 1,035 | 53.84% | 18.76% (16.61–20.60) | 7.55 (7.02–8.06) |
| `21` | 32.7 | 1,013 | 53.98% | 16.86% (15.03–19.01) | 6.47 (6.06–6.92) |
| `220` | 59.1 | 1,774 | 57.58% | 10.07% (9.04–11.12) | 5.03 (4.79–5.27) |
| `206` | 20.6 | 392 | 70.62% | 9.49% (7.19–11.75) | 4.93 (4.55–5.40) |
| `1A` | 24.5 | 736 | 81.77% | 4.83% (3.54–6.32) | 3.20 (2.85–3.61) |
| `6A` | 88.5 | 2,744 | 64.43% | 4.62% (4.03–5.18) | 3.37 (3.18–3.54) |
| `28` | 34.4 | 1,031 | 49.05% | 4.15% (3.52–4.92) | 3.40 (3.23–3.59) |
| `4` | 146.5 | 4,540 | 70.60% | 4.15% (3.74–4.59) | 3.02 (2.89–3.13) |

Linjat `24` ja `21` erottuvat selvästi. Noin joka kuudes pysäkkiohitus on yli 5
minuuttia myöhässä, eivätkä niiden luottamusvälit mene päällekkäin minkään
muun säännöllisen linjan kanssa. Seuraavana on `220`, jonka väli on kapeampi,
koska se ajaa lähes kaksi kertaa enemmän vuoroja.

Generoidun raportin p90-järjestyksen kärjessä ovat:

- `612`: 14.12 minuuttia (11.08–16.04), 37 vuoroa;
- `615`: 13.40 minuuttia, 49 vuoroa;
- `614`: 10.20 minuuttia, 74 vuoroa.

Ne ovat 2–4 vuoroa päivässä ajavia työmatkalinjoja. Niiden myöhästyminen on
todellista (myös luottamusvälien alarajat ovat korkeita), mutta se koskee harvoja
lähtöjä. Linjojen `25` ja `25A` p90 on korkeampi kuin linjan `24` (`8.10` ja
`7.75` minuuttia), mutta ne ajavat 17–18 vuoroa päivässä, juuri säännöllisen
linjan rajan alle.

Täysi taulukko: [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv).

## Etuajassa kulkevien linjojen havainnot

Etuajassa kulkeminen on merkityksellistä, koska etuajassa ohittava bussi voi
jättää odottavia matkustajia pysäkille, vaikka sen keskimääräinen viive näyttäisi
hyvältä. Etuajassa kulkemisen järjestys perustuu nyt yli 3 minuuttia etuajassa
olevien pysäkkiohitusten osuuteen. Aiemmin se perustui etuajassa kulkemisen
suuruuteen vain etuajassa olevien luokkien joukossa, mikä nosti esimerkiksi
linjan `615` sekä myöhässä että etuajassa kulkevien kärkikymmenikköön.

Säännölliset linjat:

| Linja | Vuoroja päivässä | Vuorot | Aikataulussa | >3 min etuajassa (95 %:n LV) | Mediaaniviive, min |
| --- | ---: | ---: | ---: | ---: | ---: |
| `K3` | 22.9 | 550 | 40.30% | 28.40% (26.14–30.48) | -1.52 |
| `801` | 60.2 | 1,865 | 52.44% | 19.57% (18.57–20.54) | 0.03 |
| `28` | 34.4 | 1,031 | 49.05% | 19.27% (18.33–20.07) | -0.22 |
| `26` | 55.2 | 1,381 | 59.17% | 14.05% (12.84–15.17) | -0.35 |
| `3A` | 85.5 | 1,624 | 60.09% | 13.71% (12.89–14.49) | -0.10 |
| `23` | 32.7 | 1,013 | 63.85% | 13.69% (12.50–14.95) | 0.00 |

Kaikki linjat generoidusta raportista:

| Sija | Linja | Luokat | Vuorot | Mediaaniviive | >3 min etuajassa (95 %:n LV) | p90 etuajassa |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `P6` | 3,070 | 74 | -4.22 min | 59.84% (53.98–64.79) | 15.86 min |
| 2 | `75` | 527 | 36 | -2.64 min | 46.30% (41.79–51.51) | 8.45 min |
| 3 | `N10` | 2,192 | 168 | -2.40 min | 45.30% (39.82–50.16) | 9.30 min |
| 4 | `L13` | 579 | 30 | -2.32 min | 44.39% (37.36–50.97) | 7.80 min |
| 5 | `L2` | 759 | 37 | -2.30 min | 44.14% (40.28–47.78) | 6.68 min |
| 6 | `77` | 3,888 | 88 | -1.96 min | 43.00% (35.95–50.80) | 6.95 min |
| 7 | `L6` | 2,119 | 75 | -2.16 min | 41.10% (35.15–47.54) | 8.05 min |
| 8 | `L1` | 757 | 36 | -1.68 min | 37.38% (29.76–43.07) | 7.68 min |
| 9 | `711` | 2,156 | 88 | -1.73 min | 35.30% (29.35–40.98) | 7.30 min |
| 10 | `V4` | 479 | 35 | -2.13 min | 34.45% (28.23–40.56) | 4.60 min |

`P6` on selvästi selkein etuajassa kulkemisen tapaus. Sen luottamusväli on
leveä mutta kaukana muista, eikä tulos johdu datan laadusta. `N10`, `L13` ja
`711` on merkitty puutteellisen datan linjoiksi, joten niiden sijoitukset ovat
epävarmempia. `K3` on säännöllinen linja, jolla etuajassa kulkeminen koskee
eniten lähtöjä.

Täysi taulukko: [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv).

## Kontekstikohtaiset viivekeskittymät

Kontekstimittarit ryhmittelevät linjan, suunnan, paikallisen tunnin sekä arki-
tai viikonloppupäivän mukaan. Jokainen rivi vaatii vähintään 10 eri vuoroa. Ennen
tätä sääntöä osa konteksteista perustui 1–5 vuoroon.

| Linja | Suunta | Tunti | Päivätyyppi | Luokat | Vuorot | Mediaaniviive | p90-viive | >5 min myöhässä |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `25` | 2 | 16:00 | viikonloppu | 342 | 10 | 3.76 min | 34.18 min | 42.40% |
| `402` | 2 | 22:00 | arki | 385 | 16 | -1.10 min | 27.83 min | 10.39% |
| `24` | 1 | 17:00 | viikonloppu | 392 | 18 | 3.91 min | 23.65 min | 46.17% |
| `24` | 1 | 14:00 | viikonloppu | 371 | 16 | 7.13 min | 22.95 min | 60.92% |
| `24` | 2 | 17:00 | viikonloppu | 399 | 18 | 0.58 min | 22.07 min | 28.32% |
| `901` | 2 | 12:00 | arki | 63 | 17 | 5.87 min | 19.50 min | 60.32% |
| `901` | 2 | 14:00 | arki | 254 | 13 | 1.48 min | 19.50 min | 18.11% |
| `615` | 2 | 17:00 | arki | 1,223 | 19 | 12.23 min | 19.12 min | 95.26% |
| `901` | 2 | 13:00 | arki | 614 | 13 | 1.33 min | 19.08 min | 23.29% |
| `612` | 2 | 15:00 | arki | 1,311 | 19 | 9.45 min | 16.45 min | 87.57% |

Useimmat taulukon rivit perustuvat 10–19 vuoroon. Johdonmukaisimmat signaalit
ovat:

- `615` suunta 2 klo 17:00 ja `612` suunta 2 klo 15:00 arkisin, joissa lähes
  jokainen pysäkkiohitus on yli 5 minuuttia myöhässä;
- linja `24` viikonloppuiltapäivisin, mikä tukee sen sijaa säännöllisten
  linjojen kärjessä.

Täysi taulukko: [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv).

## Tuntikohtainen viiveprofiili

Tuntiprofiili jaetaan nyt arkipäiviin ja viikonloppuihin pyhäpäivineen, ja se
kattaa jokaisen vähimmäisrajat ylittävän tunnin. Ennen tätä muutosta
Polars-versio säilytti vain 20 korkeimman p90-viiveen tuntia, ja molemmat
päivätyypit olivat sekaisin.

Arkipäivät:

| Tunti | Luokat | Vuorot | Aikataulussa | >5 min myöhässä | >3 min etuajassa | p90-viive |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 07:00 | 198,257 | 8,898 | 67.35% | 2.69% | 7.91% | 2.73 min |
| 08:00 | 198,422 | 9,080 | 66.16% | 4.42% | 7.19% | 3.18 min |
| 12:00 | 143,067 | 6,624 | 67.88% | 4.67% | 5.86% | 3.32 min |
| 13:00 | 168,444 | 7,795 | 64.99% | 5.72% | 6.94% | 3.72 min |
| 14:00 | 195,645 | 9,040 | 64.49% | 5.68% | 6.71% | 3.72 min |
| 15:00 | 211,689 | 9,683 | 62.59% | 8.40% | 5.92% | 4.55 min |
| 16:00 | 197,433 | 8,877 | 62.21% | 8.73% | 5.36% | 4.67 min |
| 17:00 | 175,669 | 7,950 | 60.47% | 4.47% | 9.88% | 2.95 min |
| 18:00 | 142,192 | 6,461 | 64.57% | 3.65% | 9.52% | 2.72 min |

Arkisin myöhästyminen kasvaa päivän mittaan ja on suurimmillaan klo 15–16.
Klo 17 se laskee jyrkästi, kun taas etuajassa kulkeminen nousee `9.88%`:iin,
päiväajan suurimpaan osuuteen. Monet bussit näyttävät kulkevan aikataulua
edellä, kun iltapäivän liikenne helpottaa. Viikonloppuisin vaihtelu on
tasaisempaa, ja myöhästymisten huippu on klo 15 (`6.44%`).

Täysi taulukko: [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv).

## Ruuhka-ajan vaikutus

Ruuhkavaikutus vertaa arkipäivän ruuhkaikkunoita (07:00–09:00 ja 15:00–18:00)
saman linjan arkipäivän päiväliikenteeseen (09:00–15:00). Aiempi vertailu
käytti kaikkia ruuhkan ulkopuolisia pysäkkiohituksia, myös öitä, viikonloppuja
ja pyhäpäiviä. Siinä ruuhkan vaikutus sekoittui hiljaisten ja vilkkaiden
jaksojen eroihin. Linjat järjestetään yli 5 minuuttia myöhässä olevien osuuden
muutoksen mukaan, ja muutokselle lasketaan vuoroittain poimittu 95 %:n
luottamusväli.

| Linja | Vertailun vuorot | Ruuhkan vuorot | >5 min myöhässä, vertailu | >5 min myöhässä, ruuhka | Muutos (95 %:n LV) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `721` | 34 | 19 | 7.03% | 39.17% | +32.13 %-yks. (21.05–42.33) |
| `722S` | 18 | 36 | 2.75% | 28.07% | +25.33 %-yks. (15.67–37.03) |
| `614` | 55 | 37 | 29.34% | 53.72% | +24.38 %-yks. (12.09–35.06) |
| `72` | 94 | 55 | 11.37% | 27.92% | +16.55 %-yks. (4.43–29.20) |
| `220` | 469 | 436 | 12.54% | 28.50% | +15.96 %-yks. (12.18–19.40) |
| `P1` | 92 | 54 | 1.05% | 13.32% | +12.27 %-yks. (6.05–19.40) |
| `722` | 91 | 78 | 11.84% | 22.19% | +10.36 %-yks. (3.27–19.02) |
| `701` | 71 | 148 | 5.71% | 15.28% | +9.57 %-yks. (1.68–15.60) |
| `28` | 261 | 256 | 2.82% | 11.19% | +8.37 %-yks. (6.12–10.47) |
| `206` | 184 | 112 | 7.00% | 15.06% | +8.06 %-yks. (1.72–13.04) |

Linjoilla `721`, `722S` ja `614` ruuhkavaikutus on suurin, mutta otokset ovat
pieniä. Linja `220` on vahvin suuren volyymin havainto: satoja vuoroja
molemmilla puolilla ja kapea luottamusväli noin 16 prosenttiyksikön kasvun
ympärillä. Linjalla `28` näkyy sama ilmiö pienemmässä mittakaavassa.

Aiemmilla ruuhkakärjillä `612` ja `615` ei ole arkipäivän liikennettä klo
09–15. Niiden aiempi ruuhkavaikutus vertasi ruuhkatunteja iltoihin ja
viikonloppuihin, joten ne eivät ole tässä taulukossa. Aiemmin toisena ollut
linja `75` perustui 36 vertailuluokkaan. Sen "kasvu" johtui siitä, että linja
kulki vähemmän etuajassa, ei siitä, että se olisi myöhästynyt enemmän.

Täysi taulukko: [`rush_impact.csv`](outputs/report-cache/rush_impact.csv).

## Päiväkohtainen verkkoprofiili

Pysäkkitason puolivälivertailu on poistettu. Sen puoliskot olivat epätasaiset:
molemmat usean päivän keräyskatkot osuivat jälkimmäiseen puoliskoon, ja
kumpaankin puoliskoon osui yksi pyhäpäivä. Listatuilla pysäkeillä oli lisäksi
vain 31–130 luokkaa puoliskoa kohden, joten suurimmat muutokset olivat pääosin
satunnaisvaihtelua. Tilalle tuli päiväkohtainen verkkoprofiili.

| Päivä | Päivätyyppi | Vuorot | Aikataulussa | >5 min myöhässä | >3 min etuajassa | Keräyskattavuus |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-04-30 | arki | 4,166 | 62.83% | 6.63% | 7.39% | 99.62% |
| 2026-05-01 | pyhä | 2,467 | 64.11% | 8.66% | 5.38% | 99.71% |
| 2026-05-08 | arki | 4,311 | 64.80% | 4.94% | 7.24% | 97.06% |
| 2026-05-09 | viikonloppu | 1,400 | 67.00% | 1.37% | 8.52% | 53.83% |
| 2026-05-10 | viikonloppu | 1,022 | 67.67% | 1.79% | 7.96% | 31.44% |
| 2026-05-13 | arki | 111 | 52.96% | 0.18% | 17.94% | 12.55% |
| 2026-05-14 | pyhä | 1,167 | 68.39% | 2.34% | 7.09% | 35.58% |
| 2026-05-19 | arki | 4,326 | 64.90% | 4.55% | 7.55% | 99.27% |

Täyden keräyskattavuuden päivinä aikataulussa olevien osuus pysyy välillä
`62.83%`–`66.89%`. Tavallisina arkipäivinä `3`–`5%` pysäkkiohituksista on yli 5
minuuttia myöhässä, ja tavallisina kokonaisina viikonloppupäivinä `1.4`–`2.6%`.
Viimeinen päivä, `2026-05-23`, päättyy iltapäivällä, joten se kattaa vain osan
päivästä.

Vappu erottuu. Myöhästyminen oli vappupäivänä `2026-05-01` jakson suurinta,
vaikka liikenne ajettiin pyhäpäivän aikataululla, ja vappuaatto oli huonoin
tavallinen arkipäivä. Molemmat sopivat kaupungin vappuruuhkiin ja
-tapahtumiin.

Matalan kattavuuden päiviä ei pidä verrata muihin. Päivältä `2026-05-13` on
dataa vain 111 vuorosta.

Täysi taulukko: [`daily_network_profile.csv`](outputs/report-cache/daily_network_profile.csv).

## Palvelutiedotteiden matched-control-havainnot

Aiemmat tiedotetulokset olivat virheellisiä kolmella tavalla:

1. **Perumiset.** `1,832` perumisrivillä ei ole voimassaoloaikaa, joten jokainen
   peruttu lähtö merkitsi koko linjansa ja kaikki sen pysäkit häiriötilaan
   aineiston loppuun asti. Yksin yksi perumisryhmä kattoi yli miljoona luokkaa.
2. **Linjanumerot.** Tiedotteen linjakenttä on GTFS-reitin tunniste, ja
   perumisen linjakenttä on julkinen linjanumero. Koodi sekoitti nämä.
   Esimerkiksi "Linja 43" -tiedote merkitsi linjan `42`, ja linjan 6 perumiset
   päätyivät linjalle `3`.
3. **Vertailuryhmät.** Vertailuryhmät oli yhdistetty, niissä saattoi olla
   muiden tiedotteiden alaisia luokkia, eikä niitä painotettu linjan ja tunnin
   mukaan.

Uudelleen rakennettu analyysi:

- kuvaa reittitunnisteet julkisiin linjanumeroihin kunakin päivänä voimassa
  olleen GTFS-version avulla;
- käyttää jokaista voimassaoloikkunaa rajattuna aikaan, jolloin tiedote
  todella näkyi aktiivisena;
- pitää perumiset vaikutuslaskennan ulkopuolella;
- jättää pois tiedotteet, jotka ovat voimassa vähintään 90 % jaksosta;
- vertaa tiedotteen alaisia luokkia saman linjan, suunnan, tunnin ja
  päivätyypin luokkiin, joilla ei ollut aktiivista tiedotetta, painottaa
  kerrokset niiden tiedoteluokkien määrällä ja laskee vuoroittain poimitut
  luottamusvälit.

Havaituista 29 tiedotteesta analysoidaan 14. Seitsemän oli pitkäkestoisia,
seitsemällä ei ollut vastaavia kohteita ja yksi ei ollut koskaan aktiivinen.

### Ryhmätason tiedotevaikutukset

| Syy | Vaikutus | Laajuus | Prioriteetti | Tiedotteet | Vuorot | Tiedoteluokat | >5 min myöhässä, tiedote | >5 min myöhässä, vertailu | Muutos (95 %:n LV) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `OTHER_CAUSE` | `UNKNOWN_EFFECT` | reitti | 900 | 1 | 35 | 881 | 15.47% | 12.46% | +3.01 %-yks. (-1.07–7.81) |
| `OTHER_CAUSE` | `DETOUR` | reitti | 1000 | 1 | 9,565 | 370,654 | 6.14% | 3.98% | +2.16 %-yks. (1.75–2.59) |
| `OTHER_CAUSE` | `OTHER_EFFECT` | reitti | 1000 | 1 | 2,513 | 144,739 | 4.95% | 3.19% | +1.76 %-yks. (1.08–2.48) |
| `OTHER_CAUSE` | `DETOUR` | pysäkki | 1000 | 1 | 11,853 | 423,483 | 4.79% | 4.03% | +0.76 %-yks. (0.46–1.07) |
| `OTHER_CAUSE` | `DETOUR` | reitti | 900 | 5 | 1,416 | 56,809 | 4.62% | 3.95% | +0.67 %-yks. (-0.73–1.85) |
| `OTHER_CAUSE` | `STOP_MOVED` | pysäkki | 900 | 1 | 2,137 | 2,142 | 6.68% | 6.14% | +0.54 %-yks. (-0.17–1.19) |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | pysäkki | 900 | 2 | 7,474 | 182,364 | 2.70% | 2.37% | +0.33 %-yks. (-0.08–0.68) |

Reittitason poikkeusreittien ja muiden reittimuutosten aikana myöhästyminen
lisääntyy vähän mutta selvästi, noin 2 prosenttiyksikköä. Pysäkkitason
poikkeusreitit lisäävät alle prosenttiyksikön. Muiden ryhmien
luottamusväleihin sisältyy nolla. Häiriötiedotteet selittävät vain pienen osan
verkon myöhästymisistä. Vertailu kuvaa yhteyttä, ei syy-seuraussuhdetta.

### Suurimmat linjatason tiedotevaikutukset

| Syy | Vaikutus | Laajuus | Linja | Vuorot | Tiedoteluokat | Muutos (95 %:n LV) |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `OTHER_CAUSE` | `DETOUR` | pysäkki | `L8` | 10 | 47 | +19.35 %-yks. (4.44–40.06) |
| `OTHER_CAUSE` | `DETOUR` | pysäkki | `N11` | 60 | 253 | +19.31 %-yks. (8.79–32.01) |
| `OTHER_CAUSE` | `DETOUR` | pysäkki | `42` | 12 | 547 | +18.96 %-yks. (-17.24–49.60) |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | pysäkki | `K1` | 12 | 320 | +17.50 %-yks. (0.00–45.16) |
| `OTHER_CAUSE` | `DETOUR` | pysäkki | `42A` | 12 | 600 | +13.43 %-yks. (-14.67–39.35) |
| `OTHER_CAUSE` | `DETOUR` | reitti | `25` | 87 | 3,976 | +9.73 %-yks. (0.77–19.57) |
| `OTHER_CAUSE` | `DETOUR` | reitti | `24` | 167 | 7,335 | +8.97 %-yks. (3.72–14.81) |
| `OTHER_CAUSE` | `DETOUR` | reitti | `25A` | 87 | 4,807 | +7.92 %-yks. (-1.39–16.77) |

Suurimmat linjatason vaikutukset perustuvat 10–12 vuoroon, ja useiden
luottamusväliin sisältyy nolla. Uskottavampia linjatason signaaleja ovat
linjojen `24` ja `25` reittitason poikkeusreitit, joissa on 167 ja 87 vuoroa ja
luottamusvälit nollan yläpuolella. Aiempi pääsignaali, linja `21`
teknisten ongelmien tiedotteiden aikana, johtui siitä, että perumiset
käsiteltiin koko linjan häiriöinä, ja se on kadonnut.

### Perutut lähdöt

Aineistossa on `74` peruttua lähtöä (`46` teknisen ongelman, `26` muun syyn ja
`2` onnettomuuden vuoksi). Eniten perumisia on linjoilla `1` (11) ja `2` (8),
sekä linjoilla `180`, `3`, `39` ja `4` (kullakin 5). Niitä on liian vähän ja ne
ovat liian lyhytkestoisia, jotta niiden vaikutusta täsmällisyyteen voisi mitata.

Täydet taulukot:
[`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
[`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
[`service_alert_inventory.csv`](outputs/report-cache/service_alert_inventory.csv),
[`service_cancellations.csv`](outputs/report-cache/service_cancellations.csv).

## Keräimen kattavuushavainnot

Välimuistissa on `86,116` keräimen kyselytietuetta. Molemmat moottorit
käyttävät nyt yhtä yhteistä katkojen tunnistinta, joten puuttuvan datan
taulukot täsmäävät.

| Lähde | Kyselyt | Virheet | Puuttuvat jaksot | Puuttuu yhteensä | Suurin puuttuva jakso | Arvioidut puuttuvat rivit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `siri_vm` | 78,203 | 191 | 71 | 4,156.67 min | 2,186.83 min | 1,122,369 |
| `siri_alerts` | 7,908 | 15 | 5 | 3,873.67 min | 2,178.08 min | 9,711 |
| `gtfs` | 5 | 0 | 0 | 0.00 min | 0.00 min | 0 |

Suurimmat katkot:

| Lähde | Katko alkaa | Katko päättyy | Puuttuva aika | Arvioidut puuttuvat rivit |
| --- | --- | --- | ---: | ---: |
| `siri_vm` | `2026-05-13T00:00:17Z` | `2026-05-14T12:27:37Z` | 2,186.83 min | 590,481 |
| `siri_alerts` | `2026-05-13T00:05:03Z` | `2026-05-14T12:28:08Z` | 2,178.08 min | 5,460 |
| `siri_vm` | `2026-05-09T10:02:27Z` | `2026-05-10T13:26:44Z` | 1,643.78 min | 443,849 |
| `siri_alerts` | `2026-05-09T10:01:41Z` | `2026-05-10T13:26:50Z` | 1,640.15 min | 4,112 |

Aineisto ei ole yhtenäinen. Päiväkohtainen profiili näyttää, mitä päiviä
katkot koskevat: `2026-05-09`, `2026-05-10`, `2026-05-13` ja `2026-05-14` jäävät
alle 95 %:n kattavuuden.

Täydet taulukot:
[`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
[`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
[`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Pandas/DuckDB ja Polars

Molemmat moottorit käyttävät nyt samoja moduuleja:

- pyhäpäiväkalenteri;
- bootstrap-luottamusvälit;
- tiedotteiden kohdistus;
- keräyskatkojen tunnistus.

Niiden laatuliput, vuorojen vähimmäismäärät ja järjestykset yhtenäistettiin.
Koko tietokannalla:

- 16 tulostaulukosta 15 täsmää täysin.
- `context_delay_metrics` eroaa enintään `0.01` yhdessä pyöristetyssä
  sarakkeessa.
- `quality_by_line` sisältää samat sarakkeet eri järjestyksessä.

Julkinen sivusto käyttää Polars-ajoa. Tämä raportti käyttää
pandas/DuckDB-ajoa. Niiden luvut ovat keskenään vaihdettavissa.

Ajoajat tällä koneella (15 GiB RAM):

| Ajo | Aika | Muistin huippu |
| --- | ---: | ---: |
| DuckDB-raportin uudelleenrakennus | noin 85 s | noin 3.8 Gt |
| Polarsin julkisen datan uudelleenrakennus | noin 2 minuuttia | noin 2.2 Gt |

## Mitä lokakuun 2026 tarkastus muutti

| Mittari | Toukokuu 2026 | Lokakuu 2026 | Syy muutokseen |
| --- | --- | --- | --- |
| Luokat / raakakyselyt | 3,746,770 / 9,837,244 | 3,729,540 / 8,582,662 | Lähtöpysäkillä odottavat ja reaaliaikadatattomat rivit poistettu |
| Konservatiivisesti poissuljetut | 5.69% | 17.72% | Sama; pelkkä odotus lähtöpysäkillä on 14.95% riveistä |
| Verkon mediaaniviive | 0.00 min | 0.02 min | Paikkamerkkinollat eivät enää lukitse mediaania |
| Verkon aikataulussa-osuus | ei raportoitu | 65.41% | Uusi aikataulussa-raja (1 min etuajassa – 3 min myöhässä) |
| Myöhässä kulkemisen kärki | `612` p90:n mukaan, 37 vuoroa | `24` säännöllisistä linjoista (18.76% >5 min myöhässä) | Järjestys osuuden mukaan; säännölliset linjat; vuorojen vähimmäismäärä ja luottamusvälit |
| Etuajassa kulkemisen kärki | `P6` etuajan p90-suuruuden mukaan | `K3` säännöllisistä linjoista; `P6` kaikista | Järjestys >3 min etuajassa olevien osuuden mukaan |
| Ruuhkakärki | `612`, +55.71 %-yks. kaikkiin ruuhkan ulkopuolisiin verrattuna | `721`, +32.13 %-yks. arkipäivän päiväliikenteeseen verrattuna | Vertailujakso; pyhäpäivät eivät enää arkipäiviä |
| Suurin tiedotesignaali | Linja `21`, tekniset ongelmat, +28.57 %-yks. | Reittitason poikkeusreitit, +2.16 %-yks. | Perumiset erotettu; reittitunnisteet kuvattu; kerrostetut vertailuryhmät |
| Pysäkkimuutokset | Puolivälijako, suurin muutos Kaamasessa | Poistettu; päiväkohtainen verkkoprofiili | Epätasaiset puoliskot, 31–130 luokkaa puoliskoa kohden |
| Tuntihuippu | 15–16, päivätyypit sekaisin | Arkisin klo 16 (8.73% >5 min myöhässä) | Jako päivätyypin mukaan; kaikki tunnit mukana |
| 1.5. ja 14.5. | Arkipäiviä | Pyhäpäiviä (viikonloppu) | GTFS-palvelukalenteri |

Muut korjaukset:

- Tasatilanteissa molemmat moottorit suosivat nyt suurempaa otosta.
- Pandasin linja–tunti-tilan viikonpäivä lasketaan nyt paikallisessa ajassa
  UTC:n sijaan.
- DuckDB:n reittikuvaus huomioi nyt päivämäärän.

Tarkistettu ja todettu oikeaksi:

- viiveen etumerkki;
- keston jäsennys;
- vuoroavaimet ja aikaleimat;
- vanhentumissuodatin;
- keräimen koodi keräysjakson aikana;
- vuorokattavuus GTFS-aikataulua vasten (98 % aikataulun arkivuoroista näkyy);
- kvantiilimenetelmä.

## Kokonaistulkinta

Toiminnallisesti hyödyllisimmät luotettavuushavainnot ovat:

1. **Myöhästyminen säännöllisillä linjoilla `24` ja `21`.** Se on selvästi
   muita säännöllisiä linjoja suurempaa, pahimmillaan arki-iltapäivisin ja
   linjalla `24` myös viikonloppuiltapäivisin. Seuraavana on linja `220`,
   jolla on parhaiten mitattu ruuhkavaikutus.
2. **Iltapäivän työmatkalinjat `612`, `615` ja `614`.** Niillä myöhästyminen on
   vakavinta, mutta ne ajavat vain 2–4 vuoroa päivässä.
3. **Etuajassa kulkeminen omana ongelmanaan.** Säännöllisistä linjoista se on
   yleisintä linjoilla `K3`, `801` ja `28`, ja äärimmäisintä palvelulinjalla
   `P6`. Etuajassa kulkeminen on suurimmillaan klo 17, kun liikenne helpottaa.
4. **Vappu.** Se tuotti kuukauden pahimmat myöhästymiset.
5. **Keräyskatkot 9.–10.5. ja 13.–14.5.** Nämä päivät on jätettävä pois
   päiväkohtaisista vertailuista.

Häiriötiedotteet selittävät vain pienen osan myöhästymisistä: poikkeusreitit
lisäävät niitä noin 2 prosenttiyksikköä siellä, missä ne ovat voimassa. Useat
aiemmat pääluvut muuttuivat tarkastuksen jälkeen merkittävästi. Erityisesti
tiedote-, ruuhka- ja pysäkkimuutososiot eivät enää tue aiempia väitteitään.
Aiempia versioita näistä havainnoista ei pidä lainata.

## Lähdeartefaktit

- Generoitu Markdown-taulukkoraportti:
  [`reports/generated/overall-results.md`](reports/generated/overall-results.md)
- Polarsin generoitu raportti:
  [`reports/generated/overall-results-polars.md`](reports/generated/overall-results-polars.md)
- Pandas/DuckDB- ja Polars-vertailu:
  [`reports/generated/pandas-polars-comparison.md`](reports/generated/pandas-polars-comparison.md)
- Välimuistin manifesti:
  [`outputs/report-cache/manifest.json`](outputs/report-cache/manifest.json)
- Välimuistin tulostaulukot:
  [`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
  [`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv),
  [`line_quality.csv`](outputs/report-cache/line_quality.csv),
  [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv),
  [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv),
  [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv),
  [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv),
  [`rush_impact.csv`](outputs/report-cache/rush_impact.csv),
  [`daily_network_profile.csv`](outputs/report-cache/daily_network_profile.csv),
  [`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
  [`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
  [`service_alert_inventory.csv`](outputs/report-cache/service_alert_inventory.csv),
  [`service_cancellations.csv`](outputs/report-cache/service_cancellations.csv),
  [`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
  [`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
  [`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Varaumat

- SIRI VM -viive on ajoneuvon arvioitu tila seuraavaa pysäkkiä lähestyttäessä,
  ei mitattu saapumis- tai lähtöaika.
- Raakarivit ovat toistuvia kyselyjä. Tulokset käyttävät matka–pysäkki-luokkia,
  mutta saman vuoron luokat korreloivat edelleen keskenään. Lue vuorojen määrät
  ja luottamusvälit, älä luokkien määriä.
- Osa paikkamerkkinollista jää jäljelle. Aikataulussa olevien osuudet voivat
  olla noin 2 prosenttiyksikköä liian suuria ja etuajassa kulkemisen osuudet
  hieman liian pieniä.
- Vain 2–4 vuoroa päivässä ajavat linjat voivat nousta vakavuusjärjestysten
  kärkeen. Niiden luottamusvälit ovat leveitä, vaikka vuorojen vähimmäismäärä
  täyttyy.
- Aineisto päättyy `2026-05-23T13:24:20Z` ja kattaa yhden kevätkuukauden. Se ei
  kerro talviolosuhteista tai myöhemmistä aikataulumuutoksista.
- Tiedotetulokset ovat vertailevia havaintotutkimuksia, eivät
  syy-seuraussuhteita.
- Keräyskatkot jättävät neljälle päivälle puutteellisen kattavuuden.
