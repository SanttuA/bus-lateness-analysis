import type { Language } from './types';

const copy = {
  fi: {
    language: 'In English',
    languageCode: 'EN',
    navFindings: 'Havainnot',
    navLines: 'Linjat',
    navStops: 'Pysäkit',
    navMethods: 'Menetelmä',
    independent: 'Riippumaton analyysi · ei Fölin virallinen palvelu',
    eyebrow: 'Turun seudun joukkoliikenne · huhti–toukokuu 2026',
    title: 'Fölin bussit: täsmällisyys datassa',
    lede: 'Missä bussit myöhästyvät, milloin aikataulupaine kasvaa ja missä etuajassa kulkeminen korostuu?',
    snapshot: 'Julkaistu tilannekuva',
    executive: 'Tiivistelmä',
    kpiBuckets: 'matka–pysäkki-luokkaa',
    kpiLines: 'linjaa',
    kpiStops: 'pysäkkiä',
    kpiP90: 'verkon p90-viive',
    kpiLate: 'yli 5 min myöhässä',
    kpiExcluded: 'raakahavainnoista suodatettu',
    definition:
      'Yksi luokka yhdistää saman ajon toistuvat ajoneuvoseurannan kyselyt samalla seuraavalla pysäkillä. Näin tiheästi näkyvä bussi ei saa analyysissä ylimääräistä painoa.',
    rankingTitle:
      'Kolme linjaa erottuu myöhästymisissä — etuajassa kulkeminen kertoo eri ongelmasta',
    rankingIntro:
      'Myöhästymisjärjestys käyttää p90-viivettä: arvoa, jonka alle 90 prosenttia luokista jää. Etuajassa kulkevat linjat järjestetään varhaisuuden p90-magnitudin mukaan.',
    lateChart: 'Korkein p90-viive linjoittain',
    lateChartSub: 'Kymmenen korkeinta arvoa, vähintään 30 matka–pysäkki-luokkaa',
    earlyChart: 'Voimakkain etuajassa kulkeminen',
    earlyChartSub: 'Kymmenen korkeinta varhaisuuden p90-arvoa',
    line: 'Linja',
    buckets: 'Luokkia',
    median: 'Mediaani',
    p90: 'p90-viive',
    earlyP90: 'Varhaisuuden p90',
    overFive: '>5 min myöhässä',
    overThreeEarly: '>3 min etuajassa',
    hourlyTitle: 'Iltapäivä nostaa verkon viiveen huipun',
    hourlyIntro:
      'Verkon tyypillinen viive pysyy lähellä nollaa, mutta korkean pään viive kasvaa selvästi kello 15–16. Viiva näyttää p90:n ja pisteviiva mediaanin.',
    hourlyChart: 'Viive paikallisen tunnin mukaan',
    hourlyChartSub: 'Kaikki linjat ja päivät, 23.4.–23.5.2026',
    explorerTitle: 'Tutki yhden linjan tuntiprofiilia',
    explorerIntro:
      'Valitse linja, suunta, päivätyyppi ja tuntiväli. Valinnat tallentuvat osoitteeseen ja ovat jaettavissa.',
    direction: 'Suunta',
    weekday: 'Arkipäivät',
    weekend: 'Viikonloput',
    dayType: 'Päivätyyppi',
    fromHour: 'Alkaen',
    toHour: 'Päättyen',
    noData: 'Tällä rajauksella ei ole vähintään 30 luokan ryhmiä.',
    rushTitle: 'Ruuhka kasvattaa viivettä eniten linjoilla 612 ja 615',
    rushIntro:
      'Vertailu pitää linjan samana ja erottaa arkipäivien ruuhkaikkunat muusta liikenteestä. Nämä ovat yhteyksiä, eivät todiste syy-seuraussuhteesta.',
    rushChart: 'Ruuhkan p90-lisä linjoittain',
    rushChartSub: 'Arkipäivien ruuhka 07–09 ja 15–18 verrattuna muihin aikoihin',
    alertsTitle: 'Häiriöt liittyvät viiveeseen, mutta eivät selitä koko vaihtelua',
    alertsIntro:
      'Häiriövertailu käyttää saman linjan, suunnan, tunnin ja päivätyypin kontrollihavaintoja. Jokainen rivi on häiriön syyn, vaikutuksen, tiedoteprioriteetin ja kohteen erillinen yhdistelmä. Ryhmätason vaikutukset jäävät pienemmiksi kuin pahimmat linja- ja ruuhkahavainnot.',
    alert: 'Häiriö',
    alertPriority: 'Tiedoteprioriteetti',
    alertPriorityDescription:
      'Fölin tiedoterajapinnassa pienempi prioriteettiluku tarkoittaa tärkeämpää viestiä. Föli suosittelee käsittelemään arvon 100 tai alle tärkeänä etusivun nostona. Luku järjestää tiedotteita, eikä mittaa viiveen suuruutta.',
    scope: 'Kohde',
    alertBuckets: 'Häiriöluokkia',
    p90Lift: 'p90-lisä',
    stopsTitle: 'Pysäkkikartta paikantaa korkean viiveen ja etuajassa kulkemisen',
    stopsIntro:
      'Kartalla on korkeintaan yksi piste pysäkkiä kohti. Taulukko tarjoaa saman tiedon ilman karttaa ja toimii tarkkaan vertailuun.',
    metric: 'Mittari',
    late: 'Myöhässä',
    early: 'Etuajassa',
    allLines: 'Kaikki linjat',
    allDays: 'Kaikki päivät',
    stopSearch: 'Pysäkin nimi tai tunnus',
    mapView: 'Kartta',
    tableView: 'Taulukko',
    stop: 'Pysäkki',
    linesServed: 'Linjoja',
    mapHint:
      'Hiirellä karttaa voi vetää; kosketusnäytöllä käytä zoomauspainikkeita. Sivun vieritys ei zoomaa karttaa.',
    changesTitle: 'Pysäkkimuutokset ovat seurantakohteita, eivät vaikutusarvioita',
    changesIntro:
      'Jakson ensimmäisen ja toisen puoliskon vertailu nostaa esiin sekä parannuksia että heikennyksiä. Muutos ei osoita tietyn toimenpiteen aiheuttaneen eroa.',
    qualityTitle: 'Mitä kannattaa tehdä seuraavaksi',
    recommendations: [
      'Seuraa linjoja 612, 615 ja 614 ensin ajosuunnan ja tunnin tasolla.',
      'Käsittele etuajassa kulkemista erillisenä matkustajaluotettavuuden mittarina.',
      'Täydennä havaintokeräystä ennen pidempien trendien tai yksittäisten päivien vertailua.',
    ],
    questionsTitle: 'Avoimet kysymykset',
    questions: [
      'Toistuvatko samat linja- ja pysäkkihavainnot pidemmällä aikavälillä?',
      'Miten sää, työmaat ja ajoaikataulujen muutokset selittävät vaihtelua?',
      'Miten SIRI-viive vastaa toteutunutta saapumisaikaa pysäkillä?',
    ],
    methodsTitle: 'Näin luvut on muodostettu',
    methodsIntro:
      'Analyysi käyttää Fölin SIRI-ajoneuvoseurantaa ja ajankohdan mukaan sovitettua GTFS-aikatauludataa. Konservatiivinen suodatus poistaa epäuskottavia, vanhentuneita sekä ajoajan ulkopuolisia rivejä.',
    caveatsTitle: 'Rajaukset ja epävarmuus',
    caveats: [
      'SIRI-viive kuvaa arvioitua ajoneuvon tilaa, ei mitattua saapumisaikaa.',
      'Konservatiivinen suodatus poistaa 5,69 % analyysiriveistä.',
      'Keräyksessä on monipäiväisiä aukkoja, joten yksittäisiä ajanjaksoja pitää tulkita varoen.',
      'Häiriö- ja pysäkkimuutokset ovat yhteyksiä, eivät kausaalisia vaikutusarvioita.',
    ],
    source: 'Lähde: Turun seudun joukkoliikenteen käyttö- ja aikataulutiedot, CC BY 4.0.',
    code: 'Lähdekoodi ja menetelmä GitHubissa',
    loading: 'Ladataan julkaistua tilannekuvaa…',
    loadError: 'Tilannekuvan lataaminen epäonnistui.',
    retry: 'Yritä uudelleen',
    tableCaption: 'Tarkat arvot samasta aineistosta',
    tableScrollHint: 'Taulukkoa voi vierittää sivusuunnassa.',
  },
  en: {
    language: 'Suomeksi',
    languageCode: 'FI',
    navFindings: 'Findings',
    navLines: 'Lines',
    navStops: 'Stops',
    navMethods: 'Method',
    independent: 'Independent analysis · not an official Föli service',
    eyebrow: 'Turku region public transport · April–May 2026',
    title: 'Föli buses: punctuality in data',
    lede: 'Where do buses run late, when does schedule pressure grow, and where is early running most visible?',
    snapshot: 'Published snapshot',
    executive: 'Executive Summary',
    kpiBuckets: 'trip–stop buckets',
    kpiLines: 'lines',
    kpiStops: 'stops',
    kpiP90: 'network p90 delay',
    kpiLate: 'over 5 min late',
    kpiExcluded: 'raw rows filtered out',
    definition:
      'One bucket combines repeated vehicle-monitoring polls for the same trip and next stop. A bus that remains visible for longer therefore receives no extra analytical weight.',
    rankingTitle: 'Three lines stand out for lateness — early running reveals a different problem',
    rankingIntro:
      'The late ranking uses p90 delay: the value below which 90% of buckets fall. Early-running lines are ranked by the p90 magnitude of early departure.',
    lateChart: 'Highest p90 delay by line',
    lateChartSub: 'Ten highest values, minimum 30 trip–stop buckets',
    earlyChart: 'Strongest early running',
    earlyChartSub: 'Ten highest p90 early-running magnitudes',
    line: 'Line',
    buckets: 'Buckets',
    median: 'Median',
    p90: 'p90 delay',
    earlyP90: 'Early p90',
    overFive: '>5 min late',
    overThreeEarly: '>3 min early',
    hourlyTitle: 'Afternoon service produces the network delay peak',
    hourlyIntro:
      'Typical network delay remains near zero, but high-end delay rises clearly at 15:00–16:00. The solid line shows p90 and the dotted line shows the median.',
    hourlyChart: 'Delay by local hour',
    hourlyChartSub: 'All lines and days, 23 April–23 May 2026',
    explorerTitle: 'Explore one line’s hourly profile',
    explorerIntro:
      'Choose a line, direction, day type, and hour range. Selections are stored in the URL and can be shared.',
    direction: 'Direction',
    weekday: 'Weekdays',
    weekend: 'Weekends',
    dayType: 'Day type',
    fromHour: 'From',
    toHour: 'To',
    noData: 'No groups in this selection meet the 30-bucket minimum.',
    rushTitle: 'Rush periods increase delay most on lines 612 and 615',
    rushIntro:
      'The comparison holds the line constant and separates weekday rush windows from other service. These are associations, not causal proof.',
    rushChart: 'Rush-period p90 lift by line',
    rushChartSub: 'Weekday rush 07–09 and 15–18 compared with other times',
    alertsTitle: 'Disruptions are associated with delay, but do not explain all variation',
    alertsIntro:
      'The disruption comparison uses controls from the same line, direction, hour, and day type. Each row is a distinct combination of disruption cause, effect, message priority, and scope. Group-level effects remain smaller than the worst line and rush-period signals.',
    alert: 'Disruption',
    alertPriority: 'Message priority',
    alertPriorityDescription:
      'In Föli’s alerts feed, a smaller priority number means a more important message. Föli recommends treating a value of 100 or below as an important front-page item. The number orders messages; it does not measure delay severity.',
    scope: 'Scope',
    alertBuckets: 'Alert buckets',
    p90Lift: 'p90 lift',
    stopsTitle: 'The stop map locates high delay and early running',
    stopsIntro:
      'The map shows no more than one marker per stop. The table exposes the same evidence without a map and supports exact comparison.',
    metric: 'Metric',
    late: 'Late',
    early: 'Early',
    allLines: 'All lines',
    allDays: 'All days',
    stopSearch: 'Stop name or ID',
    mapView: 'Map',
    tableView: 'Table',
    stop: 'Stop',
    linesServed: 'Lines',
    mapHint:
      'Drag with a mouse; use the zoom buttons on touch screens. The map does not capture page scrolling.',
    changesTitle: 'Stop-level changes are monitoring signals, not impact estimates',
    changesIntro:
      'Comparing the first and second halves of the period surfaces both improvement and deterioration. A change does not show that a particular intervention caused it.',
    qualityTitle: 'What to do next',
    recommendations: [
      'Monitor lines 612, 615, and 614 first at direction and hour level.',
      'Treat early running as a separate passenger-reliability measure.',
      'Improve collection continuity before drawing longer trends or day-specific comparisons.',
    ],
    questionsTitle: 'Further questions',
    questions: [
      'Do the same line and stop signals recur over a longer period?',
      'How much variation is explained by weather, roadworks, and timetable changes?',
      'How closely does SIRI delay match observed stop arrival time?',
    ],
    methodsTitle: 'How the numbers were produced',
    methodsIntro:
      'The analysis combines Föli SIRI vehicle monitoring with date-matched GTFS schedule data. Conservative filtering removes implausible, stale, pre-trip, and post-trip rows.',
    caveatsTitle: 'Limits and uncertainty',
    caveats: [
      'SIRI delay describes estimated vehicle state, not measured stop arrival time.',
      'Conservative filtering removes 5.69% of analysis rows.',
      'Collection includes multi-day gaps, so individual periods need cautious interpretation.',
      'Disruption and stop-change results are associations, not causal impact estimates.',
    ],
    source: 'Source: Turku Region Public Transport operating and schedule data, CC BY 4.0.',
    code: 'Source code and methodology on GitHub',
    loading: 'Loading the published snapshot…',
    loadError: 'The snapshot could not be loaded.',
    retry: 'Try again',
    tableCaption: 'Exact values from the same evidence',
    tableScrollHint: 'Scroll sideways to see every column.',
  },
} as const;

export function t(language: Language) {
  return copy[language];
}

const dataLabels = {
  fi: {
    alertCause: {
      other_cause: 'Muu syy',
      accident: 'Onnettomuus',
      technical_problem: 'Tekninen vika',
    },
    alertEffect: {
      detour: 'Poikkeusreitti',
      significant_delays: 'Merkittäviä viiveitä',
      stop_moved: 'Pysäkki siirretty',
      unknown: 'Tuntematon vaikutus',
    },
    alertScope: { route: 'Linja', stop: 'Pysäkki' },
    qualityCheck: {
      is_implausible_delay: 'Epäuskottava viive',
      is_stale_observation: 'Vanhentunut havainto',
      is_pre_trip_observation: 'Ennen ajoa tehty havainto',
      is_post_trip_observation: 'Ajon jälkeinen havainto',
      has_stop_call_disagreement: 'Pysäkkikutsun ristiriita',
      conservative_excluded_default: 'Konservatiivisesti suodatettu',
    },
    collector: { siri_vm: 'Ajoneuvoseuranta', siri_alerts: 'Häiriötiedotteet' },
  },
  en: {
    alertCause: {
      other_cause: 'Other cause',
      accident: 'Accident',
      technical_problem: 'Technical problem',
    },
    alertEffect: {
      detour: 'Detour',
      significant_delays: 'Significant delays',
      stop_moved: 'Stop moved',
      unknown: 'Unknown effect',
    },
    alertScope: { route: 'Line', stop: 'Stop' },
    qualityCheck: {
      is_implausible_delay: 'Implausible delay',
      is_stale_observation: 'Stale observation',
      is_pre_trip_observation: 'Pre-trip observation',
      is_post_trip_observation: 'Post-trip observation',
      has_stop_call_disagreement: 'Stop-call disagreement',
      conservative_excluded_default: 'Conservatively filtered',
    },
    collector: { siri_vm: 'Vehicle monitoring', siri_alerts: 'Service alerts' },
  },
} as const;

type DataLabelGroup = keyof (typeof dataLabels)['fi'];

export function dataLabel(language: Language, group: DataLabelGroup, value: string) {
  const normalized = value.toLocaleLowerCase().replaceAll(' ', '_');
  const labels = dataLabels[language][group] as Record<string, string>;
  return labels[normalized] ?? value.replaceAll('_', ' ');
}
