<!-- Space: DAT -->
<!-- Parent: Skjölun -->
<!-- Title: {{ header.api_name }} - Gagnagæði -->
# Gagna heilbrigði {{ header.api_name }}
|               |            |
| :------------ | :--------- |
| Keyrslutími | {{ header.execution.timestamp }} |
| DBT útgáfa | {{ header.dbt_version }} |
| Keyrslu auðkenni | {{ header.execution.id }} |
---
## Tölfræði 
### Heild
|               | Fjöldi prófana     | Prósenta   |
| :------------ | -----------------: | ---------: |
| Villur | {{ stats.total.error.count }} | {{ stats.total.error.percentage }} |
| Viðvaranir | {{ stats.total.warning.count }} | {{ stats.total.warning.percentage }} |
| Í lagi | {{ stats.total.ok.count }} | {{ stats.total.ok.percentage }} |
| Sleppt | {{ stats.total.skipped.count }} | {{ stats.total.skipped.percentage }} |
| **Samtals** | **{{ stats.total.total.count }}** | **{{ stats.total.total.percentage }}** |

### Vensl
| Gagnagrunnur | Skema     | Vensl                                | Villur | Prósent á villu | OK     | Heildarfjöldi prófana    |
| :----------- | :-------- | :----------------------------------- | -----: | --------------: | -----: | -----------------------: |
{% for relation in stats.relation -%} 
| {{ relation.database_name }} | {{ relation.schema_name }} | {{ relation.name }} | {{ relation.error.count }} | {{ relation.error.percentage }} | {{ relation.ok.count }} | {{ relation.total }} | 
{% endfor %}
---
## Villur
| Vensl                 | Prófun                                     | Raðir á villu  | Prósent raða á villu | Raðir        |
| :-------------------- |:------------------------------------------ | -------------: | -------------------: | -----------: |
{% for error in errors -%}    
| {{ error.relation_name }} | [{{ error.test_name }}](#{{ error.unique_id }}) | {{ error.rows_on_error }} | {{ error.rows_on_error_percentage }} | {{ error.rows_in_relation }} |
{% endfor %}
---
## Villu fyrirspurnir
{% for error in errors %}
### {{ error.unique_id }}
Gagnagrunnur: `{{ error.database_name }}`  
Skema: `{{ error.schema_name }}`  
Vensl: `{{ error.relation_name }}`  
Heiti prófunar: `{{ error.test_name }}`  
Slóð á SQL fyrirspurn: `{{ error.query_path }}`
```
{{ error.sql }}
```
{% endfor %}
---