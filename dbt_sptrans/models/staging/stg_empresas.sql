with fonte as (

    select * from {{ source('sptrans_raw', 'raw_empresas') }}

),

limpo as (

    select
        codigo_empresa,
        initcap(btrim(nome_empresa)) as nome_empresa,
        codigo_area,
        ingested_at,
        updated_at
    from fonte

)

select * from limpo