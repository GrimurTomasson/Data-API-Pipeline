<!-- Space: DAT -->
<!-- Parent: Skjölun -->
<!-- Title: {{ api_name }} - API gæði -->
# Skilgreininga heilbrigði {{ api_name }}
---
## Tölfræði 
### Heild
|                                      | Fjöldi             | Prósent af dálkum  |
| :----------------------------------- | -----------------: | -----------------: |
| **Vensl**                            | {{ stats.total.number_of_relations }}  |  | 
| Yfirskrifaðar skilgreiningar hugtaka | {{ stats.total.overwritten_concepts.count }} | {{ stats.total.overwritten_concepts.percentage }} |
| Týpu villur | {{ stats.total.type_errors.count }} | {{ stats.total.type_errors.percentage }} |
| Skjölunar villur | {{ stats.total.documentation_errors.count }} | {{ stats.total.documentation_errors.percentage }} |
| Prófana villur | {{ stats.total.test_coverage_errors.count }} | {{ stats.total.test_coverage_errors.percentage }} |
| **Villur samtals** | **{{ stats.total.errors.count }}** | **{{ stats.total.errors.percentage }}** |
---
### Vensl
Villur eru niður á dálk og fleiri en ein villutegund getur átt við sama dálkinn, við getum því verið með fleiri villur en dálkafjölda. `Dálkar í lagi` er nálgun sem og `Samtals villur %`, það fyrra getur verið núll þó svo einhverjir dálkar séu í lagi ef villurnar eru nógu margar og það síðar nefnda getur farið yfir eitt hundrað.
| Skema | Vensl | Fjöldi dálka | Yfirskrifuð hugtök |  %  | Dálkar í lagi |  %  | Týpu villur |  %  | Skjölunar villur |  %  | Prófana villur |  %  | Samtals villur |  %  |
| :---- | :---- | -----------: | -----------------: | --: | ------------: | --: | ----------: | --: | ---------------: | --: | -------------: | --: | -------------: | --: |
{% for rel in stats.relation -%}
| {{ rel.schema_name }} | {{ rel.relation_name }} | {{ rel.number_of_columns }} | {{ rel.overwritten_concepts.count }} | {{ rel.overwritten_concepts.percentage }} | {{ rel.ok_columns.count }} | {{ rel.ok_columns.percentage }} | {{ rel.type_errors.count }} | {{ rel.type_errors.percentage }} | {{ rel.documentation_errors.count }} | {{ rel.documentation_errors.percentage }} | {{ rel.test_coverage_errors.count }} | {{ rel.test_coverage_errors.percentage }} | {{ rel.errors.count }} | {{ rel.errors.percentage }} |
{% endfor %}
---
## Yfirskrifar skilgreiningar hugtaka
Lýsing skilgreinds hugtaks hefur verið yfriskrifuð í *dbt* `YAML` módeli. 
|  Skema                         | Vensl                          |  Dálkur                        |  Hugtak                        |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for concept in overwritten_concepts -%}
| {{ concept.schema_name }} | {{ concept.relation_name }} | {{ concept.column_name }} | {{ concept.concept_name }} |
{% endfor %}
---
## Villur
### Týpu
Dálkur er skilgreindur með rangri týpu eða lengd miðað við *Data Dictionary* (*DD*) skilgreiningu. Athugið að heiti dálks verður að vera í samræmi við *DD* til að við finnum þessar villur vélrænt.
|  Skema                         | Vensl                          |  Dálkur                        |  Villa                         |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for error in errors.type -%}
| {{ error.schema_name }} | {{ error.relation_name }} | {{ error.column_name }} | {{ error.message }} |
{% endfor %}

### Skjölunar
Skjölun fyrir dálk er hvorki að finna í skilgreindum hugtökum né *dbt* `YAML` módeli. Ef yfirskriftin er ekki til þess að bæta við ítarupplýsingum, fellur þetta undir villu.
|  Skema                         | Vensl                          |  Dálkur                        |  Villa                         |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for error in errors.documentation -%}
| {{ error.schema_name }} | {{ error.relation_name }} | {{ error.column_name }} | {{ error.message }} |
{% endfor %}

### Prófana
Eftirfarandi dálkar eru skilgreindir í venslum en engar prófanir eru til fyrir þá. Mögulega er ekkert `YAML` módel til fyrir venslin.
|  Skema                         | Vensl                          |  Dálkur                        |  Villa                         |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for error in errors.test_coverage -%}
| {{ error.schema_name }} | {{ error.relation_name }} | {{ error.column_name }} | {{ error.message }} |
{% endfor %}

---
## Prófanaþekja
Fjöldi prófana niður á vensl og dálka, lauslegur mælikvarði á gagnagæði.
### Vensl
Fjöldatölur prófana ná bæði yfir prófanir á stökum dálkum og venslum, en ekki yfir `SQL` prófanir.
{% for db_key, db_value in test_coverage.relation.items() -%}
{% for schema_key, schema_value in db_value.items() -%}
#### {{ db_key }}.{{ schema_key }}
| Vensl                          |  Fjöldi prófana                |
| :----------------------------- | -----------------------------: |
{% for relation_key, relation_value in schema_value.items() -%}
| {{ relation_key }} | {{ relation_value }} |
{% endfor -%}
{% endfor -%}
{% endfor -%}
---
### Vensl & Dálkar
| Gagnagrunnur           | Skema                 | Vensl                                | Dálkur                                 | Fjöldi prófana |
| :--------------------- | :-------------------- | :----------------------------------- | :------------------------------------- | -------------: |
{% for db_key, db_value in test_coverage.column.items() -%}
{% for schema_key, schema_value in db_value.items() -%}
{% for relation_key, relation_value in schema_value.items() -%}
{% for column_key, column_value in relation_value.items() -%}
| {{ db_key }} | {{ schema_key }} | {{ relation_key }} | {{ column_key }} | {{ column_value }} |
{% endfor -%}
{% endfor -%}
{% endfor -%}
{% endfor -%}

---