with fonte as (

    select * from {{ source('sptrans_raw', 'raw_paradas') }}

)

select
    codigo_parada,
    codigo_linha,
    nullif(btrim(nome_parada), '') as nome_parada,
    btrim(endereco)                as endereco,
    latitude,
    longitude,
    ingested_at,
    updated_at
from fonte