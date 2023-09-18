<!-- Space: DAT -->
<!-- Parent: Skjölun -->
<!-- Title: {{ header.api_name }} - Gagnagæði -->
# Gagna heilbrigði {{ header.api_name }}

|               |            |
| :------------ | :--------- |
| Keyrslutími | {{ header.execution.timestamp }} |
| DBT útgáfa | {{ header.dbt_version }} |
| Keyrslu auðkenni | {{ header.execution.id }} |

## Tölfræði prófana  

---

### Heild  

| Villur |    % | Viðvaranir |    % |    OK |    % |   Prófanir  |
| -----: | ---: | ---------: | ---: | ----: | ---: | ----------: |
| {{ stats.summary.error.count }} | {{ stats.summary.error.percentage }} | {{ stats.summary.warning.count }} | {{ stats.summary.warning.percentage }} | {{ stats.summary.ok.count }} | {{ stats.summary.ok.percentage }} | {{ stats.summary.total.count }} |

{% for db in stats.databases %}

#### {{ db.name }}

| Villur |    % | Viðvaranir |    % |    OK |    % | Prófanir    |
| -----: | ---: | ---------: | ---: | ----: | ---: | ----------: |
| {{ db.summary.error.count }} | {{ db.summary.error.percentage }} | {{ db.summary.warning.count }} | {{ db.summary.warning.percentage }} | {{ db.summary.ok.count }} | {{ db.summary.ok.percentage }} | {{ db.summary.total.count }} |

{% for sc in db.schemas %}

#### {{ db.name }} -> {{ sc.name }}  

| Villur |    % | Viðvaranir |    % |    OK |    % | Prófanir    |
| -----: | ---: | ---------: | ---: | ----: | ---: | ----------: |
| {{ sc.summary.error.count }} | {{ sc.summary.error.percentage }} | {{ sc.summary.warning.count }} | {{ sc.summary.warning.percentage }} | {{ sc.summary.ok.count }} | {{ sc.summary.ok.percentage }} | {{ sc.summary.total.count }} |

#### {{ db.name }} -> {{ sc.name }} -> Vensl  

| Vensl                                | Villur |    % | Viðvaranir |    % |    OK |    % | Prófanir    |
| :----------------------------------- | -----: | ---: | ---------: | ---: | ----: | ---: | ----------: |
{% for re in sc.relations -%}
| {{ re.name }} | {{ re.summary.error.count }} | {{ re.summary.error.percentage }} | {{ re.summary.warning.count }} | {{ re.summary.warning.percentage }} | {{ re.summary.ok.count }} | {{ re.summary.ok.percentage }} | {{ re.summary.total.count }} |
{% endfor %}
{% endfor %}
{% endfor %}

---

## Villur

| Vensl                 | Prófun                                     | Raðir á villu  | Prósent raða á villu | Raðir        |
| :-------------------- |:------------------------------------------ | -------------: | -------------------: | -----------: |
{% for error in errors -%}
| {{error.database_name}}.{{error.schema_name}}.{{ error.relation_name }} | [{{ error.test_name }}](#{{ error.unique_id }}) | {{ error.rows_on_error.count }} | {{ error.rows_on_error.percentage }} | {{ error.rows_in_relation }} |
{% endfor %}

---

## Villu fyrirspurnir

{% for error in errors %}

### {{ error.unique_id }}

Gagnagrunnur:           `{{ error.database_name }}`  
Skema:                  `{{ error.schema_name }}`  
Vensl:                  `{{ error.relation_name }}`  
Heiti prófunar:         `{{ error.test_name }}`  
Slóð á SQL fyrirspurn:  `{{ error.query_path }}`  

```sql
{{ error.sql }}
```

{% endfor %}

---
