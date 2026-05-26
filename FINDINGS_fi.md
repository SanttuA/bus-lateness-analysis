# Bussien myöhästymisanalyysin koontihavainnot

Tämä raportti kokoaa yhteen tämän repositorion uusimmat generoidut bussien
myöhästymisanalyysin tuotokset. Ensisijainen lähde on pandas/DuckDB-raportti
[`reports/generated/overall-results.md`](reports/generated/overall-results.md),
joka on generoitu `2026-05-26T10:11:03+00:00` uudelleen rakennetusta välimuistista.

Välimuistia ei rakennettu uudelleen tätä sanallista raporttia varten.

## Tiivistelmä

- Uusin generoitu raportti kattaa ajanjakson `2026-04-23T08:05:22Z`–
  `2026-05-23T13:24:20Z`. Aineistossa on `10,430,580` analyysiriviä
  `10,532,270` raakamuotoisesta ajoneuvohavainnosta.
- Oletuksena käytetty matka–pysäkki-tasolle niputettu aineisto sisältää
  `3,746,770` luokkaa, jotka edustavat `9,837,244` raakakyselyä `140`
  linjalta.
- Konservatiivinen laatufiltrointi sulkee pois `593,336` riviä eli `5.69%`
  analyysiriveistä. Jos myös pysäkkikutsun ristiriitarivit suljetaan pois,
  poissuljettujen osuus nousee `7.90%`:iin.
- Myöhässä kulkemisen paine kohdistuu erityisesti linjoihin `612`, `615` ja
  `614`: kullakin on korkea p90-viive ja tuhansia luokkia. Linjoilla `25`,
  `25A`, `24` ja `21` p90-viive on kärkilinjoja pienempi, mutta
  liikennemäärät ovat huomattavasti suurempia.
- Etuajassa kulkeminen on erillinen luotettavuusongelma. `P6` on selvin
  poikkeama: `92.76%` luokista on etuajassa ja `59.62%` yli kolme minuuttia
  etuajassa. Muita merkittäviä etuajassa kulkevia linjoja ovat `N10`, `75`,
  `L4`, `N7`, `L6`, `L1`, `711`, `L5` ja `L2`.
- Koko verkon myöhässä kulkemisen huippu keskittyy paikallista aikaa noin
  kello `15:00-16:00`. Molempien tuntien p90-viive on `4.40` minuuttia, ja
  niillä on suurimmat yli viisi minuuttia myöhässä olevien luokkien osuudet.
- Ruuhka-ajan vaikutus on vahvin linjoilla `612` ja `615`. Linjalla `612`
  ruuhkan p90-lisä on `10.75` minuuttia, ja yli viisi minuuttia myöhässä
  olevien luokkien osuus kasvaa `55.71` prosenttiyksikköä.
- Keräimen kattavuus ei ole enää tyhjä uusimmassa generoidussa raportissa.
  Keräintaulukot tunnistavat merkittäviä `siri_vm`- ja `siri_alerts`-keruun
  aukkoja, mukaan lukien usean päivän aukkoja, jotka voivat vaikuttaa
  kyseisten ajanjaksojen tulkintaan.
- Polars-raportti tukee pääasiallisia operatiivisia havaintoja, mutta
  pandas/DuckDB on edelleen parempi lähde tähän sanalliseen raporttiin, koska
  se on vakiintunut ensisijainen raportti ja sen nykyinen generoitu tulos on
  kattavampi.

## Rajaus ja menetelmä

| Kohta | Arvo |
| --- | --- |
| Tietokanta | `data/foli.db` |
| Generoitu raportti | `reports/generated/overall-results.md` |
| Välimuistin manifesti | `outputs/report-cache/manifest.json` |
| Välimuisti rakennettu | `2026-05-26T10:11:03+00:00` |
| Raakamuotoiset ajoneuvohavainnot | `10,532,270` |
| Analyysirivit | `10,430,580` |
| Välimuistiin tallennetut matka–pysäkki-luokat | `3,746,770` |
| Luokkien edustamat raakakyselyt | `9,837,244` |
| Edustetut linjat | `140` |
| Edustavan luokka-aineiston aikaväli | `2026-04-23T09:45:00Z`–`2026-05-23T13:39:00Z` |
| Laatutila | `conservative` |
| Luokittelutila | `trip-stop` |
| Aikavyöhyke | `Europe/Helsinki` |
| Ryhmiteltyjen havaintojen vähimmäismäärä | `30` |
| Ruuhkaikkunat | `07:00-09:00`, `15:00-18:00` paikallista aikaa arkipäivisin |

Analyysi käyttää SIRI-ajoneuvoseurannan viivearvoja. Ne ovat arvioituja
ajoneuvon tilaa kuvaavia arvoja, eivät mitattua totuutta pysäkille saapumisesta.
Raakamuotoiset ajoneuvoseurannan rivit ovat toistuvia kyselyjä, joten
oletusraportti niputtaa ne matka–pysäkki-luokiksi ennen linjojen, pysäkkien,
tuntien ja hälytyskontekstien järjestämistä.

Keskeiset viivehavainnot perustuvat robustimpiin mittareihin:

- `median_delay_min`: tyypillinen etumerkillinen viive minuutteina.
- `p90_delay_min`: viiveen yläpää; käytetään myöhässä kulkemisen
  järjestämiseen.
- `pct_over_5_min_late`: osuus luokista, jotka ovat yli viisi minuuttia
  myöhässä.
- `pct_over_3_min_early`: osuus luokista, jotka ovat yli kolme minuuttia
  etuajassa.
- `p90_early_min_abs`: etuajassa kulkemisen yläpään suuruus itseisarvona
  minuutteina.

## Datan laatuun liittyvät havainnot

Oletuksena käytettävä konservatiivinen suodatin poistaa epäuskottavat,
vanhentuneet, ennen matkaa tehdyt ja matkan jälkeen tehdyt havainnot.
Pysäkkikutsun ristiriita merkitään oletuksena lipulla, mutta sitä ei poisteta,
ellei sitä erikseen pyydetä.

| Laatutarkistus | Rivit | Osuus |
| --- | ---: | ---: |
| Analyysirivit | 10,430,580 | 100.00% |
| Epäuskottava viive | 6,637 | 0.06% |
| Vanhentunut havainto | 149,992 | 1.44% |
| Ennen matkaa tehty havainto | 343,245 | 3.29% |
| Matkan jälkeen tehty havainto | 201,581 | 1.93% |
| Pysäkkikutsun ristiriita | 320,371 | 3.07% |
| Konservatiivisen oletuksen poissulkemat | 593,336 | 5.69% |
| Konservatiivinen, kun myös pysäkkikutsun ristiriita poistetaan | 823,567 | 7.90% |

Ennen matkaa tehdyt havainnot ovat suurin yksittäinen oletuksena poistettava
ryhmä. Myös pysäkkikutsun ristiriita on merkittävä (`3.07%`), mutta sen
jättäminen lipuksi estää pääasiallisia havaintoja kaventumasta
aggressiivisemmin kuin raportin oletusasetuksilla.

### Heikoimman laadun linjat

| Linja | Rivit | Oletuksena poissuljetut | Poissuljettujen osuus |
| --- | ---: | ---: | ---: |
| `P3` | 19,454 | 11,394 | 58.57% |
| `N6` | 13,832 | 7,905 | 57.15% |
| `79A` | 6,103 | 3,423 | 56.09% |
| `711` | 12,774 | 7,150 | 55.97% |
| `L13` | 6,377 | 3,426 | 53.72% |
| `N14` | 5,035 | 2,500 | 49.65% |
| `V2` | 3,970 | 1,726 | 43.48% |
| `P6` | 16,611 | 6,683 | 40.23% |
| `N10` | 16,349 | 6,291 | 38.48% |
| `67` | 12,071 | 4,556 | 37.74% |

Useat korkean poissulkuosuuden linjat näkyvät myös operatiivisissa havainnoissa.
`P6`, `N10` ja `711` kannattaa lukea erityisellä varauksella, koska ne sijoittuvat
myös etuajassa kulkemisen poikkeamiksi.

Täydet taulukot:
[`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
[`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv).

## Myöhässä kulkevien linjojen havainnot

Myöhässä kulkemisen järjestys käyttää pääasiallisena lajitteluperusteena
p90-viivettä. Tämä nostaa esiin reitit, joilla suurimmat viiveet ovat
operatiivisesti merkittäviä, vaikka mediaani pysyisi kohtuullisena.

| Sija | Linja | Luokat | Mediaaniviive | p90-viive | >5 min myöhässä |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `612` | 2,599 | 4.17 min | 14.12 min | 46.25% |
| 2 | `615` | 5,993 | 3.03 min | 13.40 min | 39.03% |
| 3 | `614` | 6,291 | 4.02 min | 10.18 min | 39.71% |
| 4 | `42A` | 3,527 | 1.33 min | 9.72 min | 22.51% |
| 5 | `V1` | 4,235 | 3.27 min | 9.59 min | 37.21% |
| 6 | `25` | 25,175 | 2.08 min | 8.10 min | 26.72% |
| 7 | `25A` | 29,265 | 2.78 min | 7.75 min | 24.31% |
| 8 | `24` | 45,581 | 1.02 min | 7.53 min | 18.74% |
| 9 | `720` | 3,030 | 2.50 min | 7.43 min | 24.36% |
| 10 | `42` | 3,196 | 0.83 min | 7.26 min | 18.09% |

Vahvin näyttö myöhässä kulkemisesta on linjoilla `612`, `615` ja `614`: kaikilla
kolmella on korkea p90-viive ja riittävästi luokkia, jotta ne ovat vakaita
seulontasignaaleja. Myös `25`, `25A`, `24` ja `21` ovat tärkeitä, koska niiden
liikennemäärät ovat paljon suurempia; pienempikin p90-viive vaikuttaa näillä
linjoilla moniin matkoihin.

Täysi taulukko: [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv).

## Etuajassa kulkevien linjojen havainnot

Etuajassa kulkemisella on merkitystä, koska se voi aiheuttaa ohimenneitä
nousuja myös silloin, kun keskimääräinen viive näyttää hyväksyttävältä. Alla
oleva järjestys painottaa etuajassa kulkemisen yleisyyttä ja suuruutta eikä
myöhästymisviivettä.

| Sija | Linja | Luokat | Mediaaniviive | Etuajassa | >3 min etuajassa | p90-etuajan suuruus |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `P6` | 3,081 | -4.20 min | 92.76% | 59.62% | 15.87 min |
| 2 | `901` | 14,098 | 0.00 min | 49.38% | 25.51% | 13.32 min |
| 3 | `903` | 1,760 | 0.00 min | 35.74% | 18.12% | 12.38 min |
| 4 | `N10` | 2,215 | -2.60 min | 71.92% | 46.68% | 9.23 min |
| 5 | `75` | 528 | -2.67 min | 94.51% | 46.59% | 8.48 min |
| 6 | `801` | 85,169 | 0.00 min | 48.61% | 19.68% | 8.47 min |
| 7 | `L4` | 2,669 | -1.50 min | 62.27% | 34.02% | 8.28 min |
| 8 | `N7` | 2,926 | -0.68 min | 55.23% | 33.77% | 8.27 min |
| 9 | `615` | 5,993 | 3.03 min | 21.54% | 7.83% | 8.26 min |
| 10 | `L6` | 2,120 | -2.20 min | 78.96% | 41.42% | 8.03 min |

`P6` on hallitseva etuajassa kulkemisen signaali sekä yleisyydellä että
suuruudella mitattuna. Myös `N10`, `75`, `L4`, `N7`, `L6`, `L1`, `711`, `L5` ja
`L2` näyttävät korkeita etuajassa kulkemisen osuuksia. `615` näkyy sekä
myöhästymis- että etuaikajärjestyksessä, mikä viittaa suureen vaihteluun eikä
yksinkertaiseen jatkuvasti myöhässä olevaan profiiliin.

Täysi taulukko: [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv).

## Kontekstikohtaiset viivekeskittymät

Kontekstin mittarit ryhmitellään linjan, suunnan, paikallisen tunnin ja
arkipäivä/viikonloppu-jaon mukaan. Ne auttavat tunnistamaan erityisiä
liikennöintiolosuhteita, joihin viive keskittyy.

| Linja | Suunta | Tunti | Päivätyyppi | Luokat | Mediaaniviive | p90-viive | >5 min myöhässä |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| `901` | 2 | 07:00 | viikonloppu | 115 | 0.78 min | 43.34 min | 20.00% |
| `24` | 1 | 15:00 | viikonloppu | 335 | 7.37 min | 28.72 min | 69.25% |
| `901` | 2 | 06:00 | viikonloppu | 38 | 2.97 min | 28.23 min | 39.47% |
| `402` | 2 | 22:00 | arkipäivä | 394 | -1.10 min | 27.83 min | 11.42% |
| `901` | 2 | 09:00 | viikonloppu | 278 | 1.25 min | 24.65 min | 16.91% |
| `24` | 1 | 17:00 | viikonloppu | 323 | 4.88 min | 23.23 min | 48.92% |
| `24` | 2 | 17:00 | viikonloppu | 308 | 0.57 min | 20.14 min | 23.70% |
| `615` | 2 | 17:00 | arkipäivä | 1,317 | 11.73 min | 18.90 min | 88.46% |
| `612` | 2 | 15:00 | arkipäivä | 1,311 | 9.45 min | 16.45 min | 87.57% |
| `42A` | 2 | 13:00 | arkipäivä | 668 | 5.23 min | 16.16 min | 51.80% |

Kontekstitaulukossa näkyy kahdenlaisia signaaleja. Osa konteksteista on
vakavia mutta pienivolyymisia, kuten viikonlopun `901`-kontekstit. Toiset ovat
operatiivisesti vahvempia, koska sekä volyymi että vakavuus ovat korkeita:
erityisesti `615` suunta `2` arkipäivisin kello `17:00` ja `612` suunta `2`
arkipäivisin kello `15:00`.

Täysi taulukko: [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv).

## Tuntikohtainen viiveprofiili

Koko verkon mediaanit pysyvät useimpina tunteina lähellä nollaa, mutta p90-viive
ja myöhässä kulkemisen osuudet nousevat selvästi iltapäivällä.

| Tunti | Luokat | Mediaaniviive | p90-viive | >5 min myöhässä | Etuajassa |
| --- | ---: | ---: | ---: | ---: | ---: |
| 07:00 | 240,355 | 0.00 min | 2.50 min | 2.31% | 48.31% |
| 08:00 | 239,810 | 0.00 min | 2.90 min | 3.77% | 46.41% |
| 12:00 | 199,020 | 0.20 min | 3.23 min | 4.59% | 41.16% |
| 13:00 | 219,043 | 0.27 min | 3.64 min | 5.55% | 39.90% |
| 14:00 | 245,934 | 0.22 min | 3.67 min | 5.65% | 41.07% |
| 15:00 | 264,877 | 0.44 min | 4.40 min | 7.99% | 37.26% |
| 16:00 | 252,061 | 0.37 min | 4.40 min | 7.93% | 38.38% |
| 17:00 | 229,091 | -0.05 min | 2.83 min | 4.18% | 50.94% |
| 23:00 | 122,898 | -0.15 min | 1.98 min | 1.94% | 54.23% |

Selvin järjestelmätason myöhästymishuippu on kello `15:00-16:00`. Aamuhuippu
näkyy, mutta on pienempi: kello `08:00` p90-viive on suurempi kuin kello
`07:00`, mutta molemmat jäävät selvästi iltapäivän p90-arvojen alle. Myöhäisillan
ja yön jaksoissa etuajassa kulkemista näkyy enemmän kuin myöhässä kulkemista.

Täysi taulukko: [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv).

## Ruuhka-ajan vaikutus

Ruuhkavaikutus vertaa arkipäivien ruuhkaikkunoita saman linjan ei-ruuhkaisiin
jaksoihin. Generoidussa raportissa taulukko on järjestetty p90-viiveen lisän
mukaan.

| Linja | Ei-ruuhkan luokat | Ruuhkan luokat | Mediaanilisä | p90-lisä | >5 min myöhässä -lisä |
| --- | ---: | ---: | ---: | ---: | ---: |
| `612` | 769 | 1,830 | 6.60 min | 10.75 min | 55.71 pp |
| `75` | 36 | 492 | 7.32 min | 6.97 min | 0.20 pp |
| `615` | 1,944 | 4,049 | 2.58 min | 6.57 min | 27.62 pp |
| `802` | 36 | 621 | 3.33 min | 4.14 min | 4.51 pp |
| `P1` | 1,077 | 525 | 0.35 min | 4.10 min | 11.94 pp |
| `903` | 628 | 1,132 | 1.20 min | 3.91 min | 12.36 pp |
| `220` | 63,801 | 17,469 | 2.43 min | 3.75 min | 21.15 pp |
| `721` | 2,650 | 1,491 | 2.42 min | 3.63 min | 32.15 pp |
| `72` | 4,073 | 2,207 | 1.67 min | 3.32 min | 16.54 pp |
| `25` | 20,496 | 4,679 | 1.88 min | 2.57 min | 15.32 pp |

`612` on vahvin ruuhkaikkunahavainto. Sillä on sekä suuri p90-lisä että suuri
myöhässä olevien osuuden lisä. Myös `615` on merkittävä, ja sillä on suurempi
ruuhkaotos. `220`, `25`, `25A`, `24`, `28`, `722` ja `722S` ovat tärkeitä,
koska ne yhdistävät merkittäviä ruuhkavaikutuksia suuriin luokkamääriin.

Täysi taulukko: [`rush_impact.csv`](outputs/report-cache/rush_impact.csv).

## Pysäkkitason muutokset jakson puolivälissä

Puolivälin vertailu jakaa edustavan luokka-aineiston aikavälin kahteen osaan ja
vertaa toisiaan vastaavia pysäkkikonteksteja. Nämä havainnot kertovat, missä
havaittu viive muuttui välimuistiin tallennetun aikavälin ensimmäisen ja toisen
puoliskon välillä. Ne eivät todista, että jokin tietty toimenpide tai häiriö
olisi aiheuttanut muutoksen.

Generoitu taulukko on järjestetty p90-viiveen muutoksen itseisarvon mukaan,
joten se sekoittaa parannuksia ja heikennyksiä.

| Pysäkki | Lähtötason luokat | Vertailuluokat | Mediaanimuutos | p90-muutos | >5 min myöhässä -muutos |
| --- | ---: | ---: | ---: | ---: | ---: |
| Kaamanen | 50 | 42 | 0.18 min | -7.91 min | -8.86 pp |
| Koverinlahdentie | 59 | 48 | -0.01 min | -4.33 min | -9.78 pp |
| Salonkylä | 49 | 35 | -0.26 min | -4.27 min | -9.39 pp |
| Tapaninkalliontie | 51 | 41 | 6.17 min | 3.68 min | 36.01 pp |
| Virola | 81 | 65 | 0.65 min | -3.49 min | 3.27 pp |
| Elinantie | 39 | 32 | 1.15 min | -3.45 min | -8.57 pp |
| Ruusukortteli | 109 | 87 | -0.92 min | -3.43 min | -15.34 pp |
| Nummenpakan koulu | 79 | 57 | 0.00 min | -3.43 min | -6.13 pp |
| Jalkapallostadion | 39 | 31 | -1.05 min | -3.36 min | -16.63 pp |
| Vajosuontie | 97 | 74 | 5.32 min | 3.30 min | 37.81 pp |

Näytetyn taulukon selvimmät heikentymissignaalit ovat `Tapaninkalliontie` ja
`Vajosuontie`, joissa sekä mediaaniviive että yli viisi minuuttia myöhässä
olevien luokkien osuus kasvoivat selvästi. Useilla muilla pysäkeillä p90-viive
parani. Tätä osiota kannattaa käsitellä tarkemman selvityksen listana, ei
kausaalisena johtopäätöksenä.

Täysi taulukko:
[`stop_midpoint_change.csv`](outputs/report-cache/stop_midpoint_change.csv).

## Palveluhälytyksiin perustuvat matched-control-havainnot

Palveluhälytysanalyysi vertaa hälytyksiin osuneita havaintoja kontrolleihin,
jotka tulevat samasta linja-, suunta-, paikallinen tunti- ja
arkipäivä/viikonloppu-kontekstista. Tulokset ovat yhteyksiä, eivät
kausaalivaikutuksia.

### Ryhmätason hälytysvaikutukset

| Syy | Vaikutus | Laajuus | Prioriteetti | Hälytysluokat | Mediaanilisä | p90-lisä | >5 min myöhässä -lisä |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `OTHER_CAUSE` | `DETOUR` | route | 900 | 846,743 | 0.17 min | 0.90 min | 2.80 pp |
| `OTHER_CAUSE` | `UNKNOWN_EFFECT` | route | 900 | 891 | 0.34 min | 0.85 min | 3.92 pp |
| `OTHER_CAUSE` | `DETOUR` | route | 1000 | 415,259 | 0.09 min | 0.48 min | 1.63 pp |
| `OTHER_CAUSE` | `OTHER_EFFECT` | route | 1000 | 145,510 | 0.10 min | 0.40 min | 1.60 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | 1000 | 428,653 | 0.07 min | 0.27 min | 0.71 pp |
| `ACCIDENT` | `Unknown` | stop | 1200 | 196,969 | 0.08 min | 0.08 min | -0.26 pp |
| `OTHER_CAUSE` | `Unknown` | route | 1200 | 1,265,592 | 0.03 min | 0.08 min | 0.13 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | 1200 | 1,047,788 | 0.00 min | -0.10 min | -0.31 pp |

Ryhmätasolla reittikohtaisilla poikkeusreiteillä on selvin positiivinen lisä,
mutta lisä on silti maltillinen verrattuna pahimpiin linja- ja
kontekstikohtaisiin viivehavaintoihin. Pelkkä hälytyksen olemassaolo ei selitä
suurinta osaa aineiston vakavista viivekuvioista.

### Suurimmat linjatason hälytyslisät

| Syy | Vaikutus | Laajuus | Linja | Hälytysluokat | Mediaanilisä | p90-lisä | >5 min myöhässä -lisä |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `ACCIDENT` | `Unknown` | stop | `28A` | 217 | -0.57 min | 10.68 min | 17.96 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `704` | 108 | 7.00 min | 7.96 min | 78.13 pp |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | stop | `K1` | 320 | 1.10 min | 7.79 min | 17.50 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `706` | 125 | 7.22 min | 7.48 min | 62.27 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | `N11` | 268 | -0.37 min | 6.36 min | 16.83 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `21` | 25,186 | 3.45 min | 5.23 min | 28.57 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `703` | 108 | 10.26 min | 4.80 min | 77.32 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `701` | 85 | 0.87 min | 4.69 min | 12.90 pp |

Suurimmissa linjatason hälytyslisissä hälytysluokkien määrä on usein pieni.
Näytetyn taulukon operatiivisesti uskottavin suuren volyymin signaali on linja
`21` pysäkkitasoisten teknisen ongelman hälytysten aikana: `25,186`
hälytysluokkaa, `3.45` minuutin mediaanilisä, `5.23` minuutin p90-lisä ja
`28.57` prosenttiyksikön lisä myöhässä olevien osuuteen.

Täydet taulukot:
[`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
[`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv).

## Keräimen kattavuushavainnot

Uusin välimuistin manifesti sisältää `86,116` keräimen kyselytietuetta. Nykyiset
keräintuotokset tunnistavat merkittäviä puuttuvan datan jaksoja, joten keruun
kattavuus on todellinen varauma kyseisille päivämääräväleille.

| Lähde | Kyselyt | Epäonnistumiset | Puuttuvat kohdat | Puuttuva aika yhteensä | Suurin aukko | Arvioidut menetetyt rivit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `siri_vm` | 78,203 | 191 | 71 | 4,156.67 min | 2,186.83 min | 1,122,360.85 |
| `siri_alerts` | 7,908 | 15 | 5 | 3,873.67 min | 2,178.08 min | 9,711.16 |
| `gtfs` | 5 | 0 | 0 | 0.00 min | 0.00 min | 0.00 |

Kaksi suurinta näkyvää aukkoa ovat:

| Lähde | Aukon alku | Aukon loppu | Puuttuva aika | Arvioidut menetetyt rivit |
| --- | --- | --- | ---: | ---: |
| `siri_vm` | `2026-05-13T00:00:17Z` | `2026-05-14T12:27:37Z` | 2,186.83 min | 590,477.01 |
| `siri_alerts` | `2026-05-13T00:05:03Z` | `2026-05-14T12:28:08Z` | 2,178.08 min | 5,460.39 |
| `siri_vm` | `2026-05-09T10:02:27Z` | `2026-05-10T13:26:44Z` | 1,643.78 min | 443,845.56 |
| `siri_alerts` | `2026-05-09T10:01:41Z` | `2026-05-10T13:26:50Z` | 1,640.15 min | 4,111.81 |

Nämä aukot tarkoittavat, ettei raporttia pidä tulkita täydellisenä jatkuvana
kattavuutena. Ne eivät mitätöi koko analyysiä, mutta ne ovat tärkeitä
päivämäärä- tai häiriökohtaisessa tulkinnassa aukkojen ajalta.

Täydet taulukot:
[`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
[`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
[`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Pandas/DuckDB- ja Polars-lähteiden vertailu

Erillinen vertailuraportti
[`reports/generated/pandas-polars-comparison.md`](reports/generated/pandas-polars-comparison.md)
osoittaa, että molemmat raportointipolut käyttävät samaa SQLite-tietokantaa,
havaintojen aikaväliä, analyysirivien määrää, raakahavaintojen määrää,
luokkien määrää, laatutilaa, luokittelutilaa, aikavyöhykettä ja
havaintojen vähimmäismääräasetuksia.

Käytännön tulkinnan kannalta tärkeää on, että ensisijaiset operatiiviset
havainnot ovat enimmäkseen yhteneviä: datan laatutulokset, myöhässä kulkevien
linjojen järjestys, etuajassa kulkevien linjojen järjestys, ruuhkavaikutus ja
monet kontekstimittarit osoittavat samoihin reitteihin ja aikaikkunoihin. Tämä
tekee Polars-tuotoksesta hyödyllisen tukilähteen laajoille johtopäätöksille.

Polars oli olemassa olevissa generoiduissa ajoissa nopeampi, mutta vain `2.22`
sekuntia kokonaisajassa eli noin `1.0%`. Suurin osa ajoajasta kuluu
välimuistin/rakennuksen työhön, joten nopeus ei ole ratkaiseva ero tässä
nykyisessä raportointiprosessissa.

Tässä sanallisessa raportissa pandas/DuckDB on parempi datalähde. Se on
vakiintunut ensisijainen generoitu raportti, sen nykyinen raporttituotos on
kattavampi sanallisia osioita varten, ja se on turvallisempi lähde silloin, kun
hälytysten, tuntiprofiilien ja keräimen tulkinta on tärkeää. Polarsia kannattaa
käsitellä hyödyllisenä vahvistavana lähteenä, kunnes jäljellä olevat
tuotoserot on ratkaistu.

## Kokonaistulkinta

Toiminnallisesti hyödyllisimmät luotettavuushavainnot keskittyvät neljään
alueeseen:

1. Myöhässä kulkemisen prioriteettilinjat: `612`, `615`, `614`, `42A`, `V1`,
   `25`, `25A`, `24`, `720` sekä suuren volyymin linja `21`.
2. Etuajassa kulkemisen prioriteettilinjat: `P6`, `N10`, `75`, `L4`, `N7`,
   `L6`, `L1`, `711`, `L5` ja `L2`.
3. Aika- ja kontekstikohtaiset keskittymät: iltapäiväruuhkan kontekstit,
   erityisesti `612` suunta `2` arkipäivisin kello `15:00` ja `615` suunta `2`
   arkipäivisin kello `17:00`.
4. Keräimen aukot: useiden tuntien tai päivien aukot `siri_vm`- ja
   `siri_alerts`-keruussa, jotka voivat vaikuttaa päivämääräkohtaisiin
   johtopäätöksiin.

Ylätason verkkomediaani on yleensä lähellä nollaa, joten pelkät keskiarvot
peittäisivät tärkeimmät kuviot. Operatiiviset ongelmat näkyvät viiveen
yläpäässä, etuajassa kulkemisen osuuksissa, linja–suunta–tunti-konteksteissa ja
keruun kattavuudessa.

Datan laatu on riittävä laajaan seulontaan, mutta ei täydellinen. Linjat, joilla
konservatiivisen suodatuksen poissulkuosuus on korkea, pitäisi validoida ennen
linjatason päätöksiä. Pysäkkitason puoliväli- ja palveluhälytystuloksia
kannattaa käyttää selvityksen johtolankoina, koska ne riippuvat sovitetuista
kontekstimäärityksistä ja voivat olla herkkiä otoskoolle.

## Lähdeartefaktit

- Generoitu Markdown-taulukkoraportti:
  [`reports/generated/overall-results.md`](reports/generated/overall-results.md)
- Generoitu Polars-raportti:
  [`reports/generated/overall-results-polars.md`](reports/generated/overall-results-polars.md)
- Pandas/DuckDB:n ja Polarsin vertailu:
  [`reports/generated/pandas-polars-comparison.md`](reports/generated/pandas-polars-comparison.md)
- Välimuistin manifesti:
  [`outputs/report-cache/manifest.json`](outputs/report-cache/manifest.json)
- Välimuistiin tallennetut tulostaulukot:
  [`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
  [`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv),
  [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv),
  [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv),
  [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv),
  [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv),
  [`rush_impact.csv`](outputs/report-cache/rush_impact.csv),
  [`stop_midpoint_change.csv`](outputs/report-cache/stop_midpoint_change.csv),
  [`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
  [`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
  [`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
  [`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
  [`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Varaumat

- SIRI VM -viive on arvioitu ajoneuvoseurannan tilatieto, ei mitattu totuus
  pysäkille saapumisesta.
- Raakamuotoiset ajoneuvoseurannan rivit ovat toistuvia kyselyjä; oletustulokset
  käyttävät matka–pysäkki-luokkia, jotta näkyvissä oleva ajoneuvo ei saa
  ylipainoa vain siksi, että sitä kyseltiin toistuvasti.
- Konservatiivinen suodatus poistaa epäuskottavat, vanhentuneet, ennen matkaa ja
  matkan jälkeen tehdyt rivit. Pysäkkikutsun ristiriita merkitään lipulla, mutta
  sitä ei poisteta oletusvälimuistissa.
- Uusin generoitu lähdedata päättyy aikaan `2026-05-23T13:24:20Z`; tätä
  myöhempää dataa ei ole mukana raportissa.
- Palveluhälytys- ja pysäkkien puolivälitulokset ovat sovitettuja
  havainnointivertailuja. Niitä ei pidä tulkita kausaaliseksi todisteeksi.
- Joissakin korkealle sijoittuneissa havainnoissa otoskoot ovat pieniä ja
  lähellä `30` luokan vähimmäisrajaa. Ne pitäisi validoida ennen operatiivista
  priorisointia.
- Keräimen katkoksia ja puuttuvaa dataa kuvaavat tuotokset osoittavat
  merkittäviä aukkoja. Ole varovainen tulkitessasi päivämääräkohtaisia
  kuvioita näiden jaksojen aikana.
