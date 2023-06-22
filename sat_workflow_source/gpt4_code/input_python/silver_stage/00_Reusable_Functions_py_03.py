
#explode column logic
def specified_column(df,explode_column):
    dyn_class=explode_column+'_dyn_class'
    href=explode_column+'_href'
    df=(
        df
        .withColumn(explode_column,explode_outer(col(explode_column)))
        .withColumn(dyn_class,col(explode_column)['_dyn_class'])
        .withColumn(href,col(explode_column)['_href'])
        .withColumn(href,split(col(href),'#').getItem(1))
        .drop(explode_column)
    )
    return df

