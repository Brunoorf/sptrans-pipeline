with posicoes as (

    select
        prefixo_veiculo,
        codigo_linha,
        letreiro,
        momento_local,
        data_local,
        hora_local,
        tipo_dia,
        latitude,
        longitude
    from {{ ref('stg_posicoes') }}

),

com_anterior as (

    select
        *,
        lag(latitude)      over w as lat_anterior,
        lag(longitude)     over w as lon_anterior,
        lag(momento_local) over w as momento_anterior
    from posicoes
    window w as (partition by prefixo_veiculo order by momento_local)

),

deslocamento as (

    select
        *,
        extract(epoch from (momento_local - momento_anterior)) as segundos,

        2 * 6371000 * asin(sqrt(
            power(sin(radians(latitude - lat_anterior) / 2), 2)
            + cos(radians(lat_anterior))
            * cos(radians(latitude))
            * power(sin(radians(longitude - lon_anterior) / 2), 2)
        )) as metros

    from com_anterior
    where momento_anterior is not null

),

velocidade as (

    select
        prefixo_veiculo,
        codigo_linha,
        letreiro,
        data_local,
        hora_local,
        tipo_dia,
        momento_local,
        latitude,
        longitude,
        segundos,
        round(metros)::numeric                         as metros,
        round(((metros / segundos) * 3.6)::numeric, 1) as kmh,
        ((metros / segundos) * 3.6) < 1                as parado
    from deslocamento
    where segundos between 30 and 1200
      and metros < 20000

)

select * from velocidade