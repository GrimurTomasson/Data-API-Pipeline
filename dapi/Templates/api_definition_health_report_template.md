<!-- Space: DAT -->
<!-- Parent: Skjölun -->
<!-- Title: {{ api_name }} - API gæði -->
# Skilgreininga heilbrigði {{ api_name }}
---
## Tölfræði 
### Heild
|                                      | Fjöldi             | Prósenta   |
| :----------------------------------- | -----------------: | ---------: |
| Vensl                                | {{ stats.total.number_of_relations }}  |  | 
| Yfirskrifaðar skilgreiningar hugtaka | {{ stats.total.overwritten_concepts.count }} | {{ stats.total.overwritten_concepts.percentage }} |
| Týpu villur | {{ stats.total.type_errors.count }} | {{ stats.total.type_errors.percentage }} |
| Skjölunar villur | {{ stats.total.documentation_errors.count }} | {{ stats.total.documentation_errors.percentage }} |
| **Villur samtals** | **{{ stats.total.errors.count }}** | **{{ stats.total.errors.percentage }}** |
---
### Vensl
| Skema | Vensl | Fjöldi dálka | Yfirskrifuð hugtök |  %  | Dálkar í lagi |  %  | Týpu villur |  %  | Skjölunar villur |  %  | Samtals villur |  %  |
| :---- | :---- | -----------: | -----------------: | --: | ------------: | --: | ----------: | --: | ---------------: | --: | -------------: | --: |
{% for rel in stats.relation -%}
| {{ rel.schema_name }} | {{ rel.relation_name }} | {{ rel.number_of_columns }} | {{ rel.overwritten_concepts.count }} | {{ rel.overwritten_concepts.percentage }} | {{ rel.ok_columns.count }} | {{ rel.ok_columns.percentage }} | {{ rel.type_errors.count }} | {{ rel.type_errors.percentage }} | {{ rel.documentation_errors.count }} | {{ rel.documentation_errors.percentage }} | {{ rel.errors.count }} | {{ rel.errors.percentage }} |
{% endfor %}
---
## Yfirskrifar skilgreiningar hugtaka
|  Skema                         | Vensl                          |  Dálkur                        |  Hugtak                        |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for concept in overwritten_concepts -%}
| {{ concept.schema_name }} | {{ concept.relation_name }} | {{ concept.column_name }} | {{ concept.concept_name }} |
{% endfor %}
---
## Villur
### Týpu
|  Skema                         | Vensl                          |  Dálkur                        |  Villa                         |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for error in errors.type -%}
| {{ error.schema_name }} | {{ error.relation_name }} | {{ error.column_name }} | {{ error.message }} |
{% endfor %}

### Skjölunar
|  Skema                         | Vensl                          |  Dálkur                        |  Villa                         |
| :----------------------------- | :----------------------------- | :----------------------------- | :----------------------------- |
{% for error in errors.documentation -%}
| {{ error.schema_name }} | {{ error.relation_name }} | {{ error.column_name }} | {{ error.message }} |
{% endfor %}
---