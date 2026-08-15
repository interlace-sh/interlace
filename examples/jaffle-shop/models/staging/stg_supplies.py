"""stg_supplies — the one staging model that needed more than a substitution.

dbt builds its key with `{{ dbt_utils.generate_surrogate_key(['id', 'sku']) }}`, a
package macro. There is no package to install, so the expression is written once in
`_macros.py` and this model generates its SQL from it.

The rest of the model is dbt's, unchanged.
"""

from _macros import cents_to_dollars, surrogate_key

from interlace.dsl.decorators import REGISTRY, ModelDef

REGISTRY.register_model(
    ModelDef(
        name="stg_supplies",
        materialise="view",
        sql=f"""
with

source as (

    select * from raw_supplies

),

renamed as (

    select

        ----------  ids
        {surrogate_key(["id", "sku"])} as supply_uuid,
        id as supply_id,
        sku as product_id,

        ---------- text
        name as supply_name,

        ---------- numerics
        {cents_to_dollars("cost")} as supply_cost,

        ---------- booleans
        perishable as is_perishable_supply

    from source

)

select * from renamed
""",
        checks=[{"not_null": "supply_uuid"}, {"unique": "supply_uuid"}],
    )
)
