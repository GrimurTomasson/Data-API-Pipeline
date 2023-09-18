<!-- Space: DAT -->
<!-- Parent: Skjölun -->
<!-- Title: Reference Data API -->

# Inngangur
Hlutverk þessa kóðasafns er að viðhalda samhæfðu og lógísku gagnaviðmóti ofan á gögn hverra tilurð er utan fyrirtækisins, m.ö.o. ytri gögn.
Þessi gögn má nota allstaðar í gegnum REFERENCE-DATA-API og einnig má afrita þau og nota. Séu gögnin afrituð og þörf er fyrir viðbætur þurfa þær að fara í sér töflur í viðkomandi API, ekki má breyta töflunum sem koma úr þessum API. Ef þessu er fylgt er hægt að nota viðkomandi API án vandræða í gegnum gagnamiðju (e:analytical platform).
Dæmi um gögn sem eiga heima í þessum API eru póstnúmer, sveitarfélög o.s.frv. Gögn sem eru samnýtt af mörgum sviðum/deildum/hópum en eiga uppruna sinn í hjá okkur, þó það sé einungis að hluta til, eiga heima í MASTER-DATA-API. Þetta á þó ekki við útreiknuð gildi eða gagnahreinsun á reference gögnum.

Athugið að ekkert sem tengist viðmótum, skýrslum eða samantektum þvert á mörg hugtök á heima hér.
Öll vensl sem snúa að hugtökum, en ekki tegundum þeirra eða tengslum, eru útgáfustýrð. Það sama á við um gögn þeirra.  

Gögn þessa viðmóts eru inntak í skýrslugerð, stjórnborð og aðrar greiningar.  

Þetta skjal er búið til vélrænt, allar handvirkar breytingar munu tapast.

# API Vensl
Venslin í þessu viðmóti skiptast í tvo flokka, nústöðu og söguleg gögn. Nústöðu gögnin eru öll í samnefndu skema, **Nustada**. Athugið að nústaða gagna er nýjasta staða sem við eigum, það er ekki gefið að þetta séu rauntímagögn. Söguleg gögn innihalda gagnasögu og eru í skema sem heitir **Saga**. Athugið að sagan er uppfærð daglega, ef gögnin breytast oftar en það koma þær breytingar ekki hér inn. Þetta er gagnasaga til almennrar greiningar, ekki auditing eða breytingasaga.

{% for rel in relations -%}
## {{ rel.schema_name }}.{{ rel.relation_name }}
{{ rel.description }}  

| Dálkur        | Lýsing        | Týpa          | Lengd         | Uppruni lýsingar |
| :------------ | :------------ | :------------ | :------------ | :------------    |
{% for col in rel.columns -%}
    {%- if col.description.missing == True -%}
| {{ col.name }} | **Skjölun vantar!** | {{ col.type.name }} | {{ col.type.length }} |  |
    {%- else -%}
| {{ col.name }} | {{ col.description.text }} | {{ col.type.name }} | {{ col.type.length }} | {{ col.description.origin }} |
    {%- endif %}
{% endfor %}
{% endfor %}