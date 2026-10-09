select
    codigo_corredor,
    btrim(nome_corredor) as nome_corredor,
    ingested_at,
    updated_at
from {{ source('sptrans_raw', 'raw_corredores') }}