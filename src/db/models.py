from sqlalchemy import (
    Table, Column, Integer, String, Float, 
    MetaData, ForeignKey, Boolean, DateTime, 
    PrimaryKeyConstraint, Text, ARRAY, Date
)

metadata = MetaData()

#|--------------------------------------------------------------------------------------------|
#|                                      DOMAIN: EXCHANGE                                      |
#|--------------------------------------------------------------------------------------------|

# Dimension: Basic exchange identifiers
dim_exchange_id = Table(
    "dim_exchange_id", metadata,
    Column("exchange_id", Integer, primary_key=True),     # From /exchange/map -> cast to int (Spark)
    Column("name", String, nullable=False),               # From /exchange/map -> string
)

# Dimension: Time-variant exchange info
dim_exchange_map = Table(
    "dim_exchange_map", metadata,
    Column("exchange_id", Integer, ForeignKey("dim_exchange_id.exchange_id")),   # From /exchange/info
    Column("snapshot_timestamp", DateTime),                                      # Pipeline -> to_timestamp (Spark)
    Column("snapshot_str", String, nullable=False),                              # Pipeline -> date_format (Spark), corresponds to last_updated
    Column("is_active", Boolean),                                                # From /exchange/map -> boolean (Spark)
    Column("maker_fee", Float),                                                  # From /exchange/info -> float (Spark)
    Column("taker_fee", Float),                                                  # From /exchange/info -> float (Spark)
    Column("weekly_visits", Integer),                                            # From /exchange/info -> int (Spark)
    PrimaryKeyConstraint("exchange_id", "snapshot_timestamp")
)

# Dimension: Static details about the exchange
dim_exchange_info = Table(
    "dim_exchange_info", metadata,
    Column("exchange_id", Integer, ForeignKey("dim_exchange_id.exchange_id"), primary_key=True),  # From /exchange/info
    Column("slug", String(255)),                                          # From /exchange/info
    Column("description", Text),                                          # From /exchange/info
    Column("date_launched", String(25)),                                  # From /exchange/info -> formatted date (Spark)
    Column("logo_url", String(500)),                                      # From /exchange/info
    Column("website_url", String(500)),                                   # From /exchange/info -> urls.website[0] (via Spark)
    Column("twitter_url", String(500)),                                   # From /exchange/info -> urls.twitter[0] (via Spark)
    Column("blog_url", String(500)),                                      # From /exchange/info -> urls.blog[0] (via Spark)
    Column("fee_url", String(500)),                                       # From /exchange/info -> urls.fee[0] (via Spark)
    Column("fiats", ARRAY(String))                                        # From /exchange/info -> split into array via Spark
)

# Fact table: Assets held by each exchange wallet at a point in time
fact_exchange_assets = Table(
    "fact_exchange_assets", metadata,
    Column("exchange_id", Integer, ForeignKey("dim_exchange_id.exchange_id")),       # From /exchange/assets
    Column("snapshot_timestamp", DateTime),                                          # Pipeline -> to_timestamp (Spark)
    Column("snapshot_str", String, nullable=False),                                  # Pipeline -> date_format (Spark)
    Column("spot_volume_usd", Float),                                                # From /exchange/info -> DoubleType (Spark)
    Column("blockchain_symbol", String(20)),                                         # From /exchange/assets
    Column("blockchain_name", String(100)),                                          # From /exchange/assets
    Column("wallet_address", String(255)),                                           # From /exchange/assets
    Column("crypto_id", Integer, ForeignKey("dim_crypto_id.crypto_id")),             # From /exchange/assets
    Column("balance", Float),                                                        # From /exchange/assets -> DoubleType
    Column("currency_price_usd", Float),                                             # From /exchange/assets -> DoubleType
    Column("total_usd_value", Float),                                                # KPI (Spark) = balance * currency_price_usd
    Column("wallet_weight", Float),                                                  # KPI (Spark) = total_usd_value / spot_volume_usd
    PrimaryKeyConstraint("exchange_id", "wallet_address", "crypto_id", "snapshot_timestamp")
)

#|--------------------------------------------------------------------------------------------|
#|                                      DOMAIN: CRYPTO                                        |
#|--------------------------------------------------------------------------------------------|

# Dimension: Core crypto identity (static ID and platform info)
dim_crypto_id = Table(
    "dim_crypto_id", metadata,
    Column("crypto_id", Integer, primary_key=True),                # /cryptocurrency/map -> id
    Column("name", String(255)),                                   # /cryptocurrency/map -> name
    Column("symbol", String(20)),                                  # /cryptocurrency/map -> symbol
    Column("platform_id", Integer),                                # /cryptocurrency/map -> platform.id
    Column("platform_name", String(255)),                          # /cryptocurrency/map -> platform.name
    Column("platform_symbol", String(20)),                         # /cryptocurrency/map -> platform.symbol
    Column("platform_token_address", String(255))                  # /cryptocurrency/map -> platform.token_address
)

# Dimension: Time-variant crypto metadata (e.g., rank, activity)
dim_crypto_map = Table(
    "dim_crypto_map", metadata,
    Column("crypto_id", Integer, ForeignKey("dim_crypto_id.crypto_id")),  # /cryptocurrency/map -> id
    Column("snapshot_timestamp", DateTime),                               # pipeline -> to_timestamp
    Column("snapshot_str", String, nullable=False),                       # pipeline -> date_format (lisible)
    Column("rank", Integer),                                              # /cryptocurrency/map -> rank
    Column("is_active", Integer),                                         # /cryptocurrency/map -> is_active
    Column("last_historical_data", String),                               # /cryptocurrency/map -> last_historical_data
    PrimaryKeyConstraint("crypto_id", "snapshot_timestamp")
)

# Dimension: Static and descriptive information about each crypto
dim_crypto_info = Table(
    "dim_crypto_info", metadata,
    Column("crypto_id", Integer, ForeignKey("dim_crypto_id.crypto_id"), primary_key=True),  # /cryptocurrency/info -> id
    Column("slug", String(50)),                                   # /cryptocurrency/info -> slug
    Column("snapshot_timestamp", DateTime),                       # from date_snapshot -> to_timestamp
    Column("snapshot_date", Date),                                # Spark formatted date (String)
    Column("date_launched", Date),                                # /cryptocurrency/info -> date_launched
    Column("date_added", Date),                                   # /cryptocurrency/info -> date_added
    Column("category", String(50)),                               # /cryptocurrency/info -> category
    Column("tags", ARRAY(String)),                                # /cryptocurrency/info -> tags (Spark -> array[string])
    Column("description", Text),                                  # /cryptocurrency/info -> description
    Column("logo_url", String(500)),                              # /cryptocurrency/info -> logo
    Column("website_url", String(500)),                           # /cryptocurrency/info -> urls.website[0]
    Column("technical_doc_url", String(500)),                     # /cryptocurrency/info -> urls.technical_doc[0]
    Column("explorer_url", String(500)),                          # /cryptocurrency/info -> urls.explorer[0]
    Column("source_code_url", String(500)),                       # /cryptocurrency/info -> urls.source_code[0]
    Column("twitter_url", String(500))                            # /cryptocurrency/info -> urls.twitter[0]
)

# Fact table: Market data and KPIs for each crypto at a point in time
fact_crypto_market = Table(
    "fact_crypto_market", metadata,
    
    # Identifiers and context
    Column("crypto_id", Integer, ForeignKey("dim_crypto_id.crypto_id")),  # /cryptocurrency/listings/latest -> id
    Column("snapshot_timestamp", DateTime),                                     # Pipeline -> to_timestamp(date_snapshot)
    Column("snapshot_str", String, nullable=False),                             # Pipeline -> date_format (lisible)
    Column("batch_index", Integer),                                             # Pipeline batch index (for traceability)

    # General metadata
    Column("cmc_rank", Integer),                                                # /cryptocurrency/listings/latest -> cmc_rank
    Column("last_updated_usd", String),                                         # /cryptocurrency/listings/latest -> last_updated (Spark format)
    Column("platform_id", Integer),                                             # /cryptocurrency/listings/latest -> platform.id
    Column("num_market_pairs", Integer),                                        # /cryptocurrency/listings/latest -> num_market_pairs

    # Financial data
    Column("price_usd", Float),                                                 # -> price
    Column("volume_24h_usd", Float),                                            # -> volume_24h
    Column("volume_change_24h", Float),                                         # -> volume_change_24h
    Column("percent_change_1h_usd", Float),                                     # -> percent_change_1h
    Column("percent_change_24h_usd", Float),                                    # -> percent_change_24h
    Column("percent_change_7d_usd", Float),                                     # -> percent_change_7d
    Column("market_cap_usd", Float),                                            # -> market_cap
    Column("market_cap_dominance_usd", Float),                                  # -> market_cap_dominance
    Column("fully_diluted_market_cap_usd", Float),                              # -> fully_diluted_market_cap
    Column("self_reported_market_cap", Float),                                  # -> self_reported_market_cap

    # Supply data
    Column("circulating_supply", Float),                                        # -> circulating_supply
    Column("total_supply", Float),                                              # -> total_supply
    Column("max_supply", Float),                                                # -> max_supply
    Column("infinite_supply", Boolean),                                         # -> infinite_supply
    Column("self_reported_circulating_supply", Float),                          # -> self_reported_circulating_supply

    # Derived KPIs (calculated in Spark)
    Column("volume_to_market_cap_ratio", Float),                                # KPI = volume_24h / market_cap
    Column("missing_supply_ratio", Float),                                      # KPI = (total - circulating) / total
    Column("supply_utilization", Float),                                        # KPI = circulating / max
    Column("fully_diluted_cap_ratio", Float),                                   # KPI = fully_diluted / market_cap

    PrimaryKeyConstraint("crypto_id", "snapshot_timestamp")
)

#|--------------------------------------------------------------------------------------------|
#|                                      DOMAIN: CRYPTO CATEGORY                               |
#|--------------------------------------------------------------------------------------------|

# Dimension: Static category definitions (e.g., DeFi, NFT)
dim_crypto_category = Table(
    "dim_crypto_category", metadata,
    
    Column("category_id", String(100), primary_key=True), # /cryptocurrency/categories -> category_id
    Column("name", String(100)),                          # /cryptocurrency/categories -> name
    Column("title", String(255)),                         # /cryptocurrency/categories -> title
    Column("description", Text)                           # /cryptocurrency/categories -> description
)

# Link table: Many-to-many relation between crypto and categories
crypto_category_link = Table(
    "crypto_category_link", metadata,

    # cryptocurrency/category -> crypto_id : FK to crypto
    Column("crypto_id", Integer, ForeignKey("dim_crypto_id.crypto_id"), nullable=False),        

    # cryptocurrency/category -> category_id : FK to categoy
    Column("category_id", String(100), ForeignKey("dim_crypto_category.category_id"), nullable=False), 

    PrimaryKeyConstraint("crypto_id", "category_id")
)

# Fact table: Aggregated market data per category over time
fact_crypto_category = Table(
    "fact_crypto_category", metadata,
    Column("category_id", String(100), ForeignKey("dim_crypto_category.category_id")), # /cryptocurrency/categories -> category_id
    Column("snapshot_timestamp", DateTime),                          # Spark -> to_timestamp
    Column("snapshot_str", String, nullable=False),                  # Spark -> date_format lisible
    Column("num_tokens", Integer),                                   # /cryptocurrency/categories -> num_tokens
    Column("avg_price_change", Float),                               # /cryptocurrency/categories -> avg_price_change
    Column("market_cap", Float),                                     # /cryptocurrency/categories -> market_cap
    Column("market_cap_change", Float),                              # /cryptocurrency/categories -> market_cap_change
    Column("volume", Float),                                         # /cryptocurrency/categories -> volume
    Column("volume_change", Float),                                  # /cryptocurrency/categories -> volume_change
    Column("last_updated", String),                                  # /cryptocurrency/categories -> last_updated -> date_format (Spark)
    Column("volume_to_market_cap_ratio", Float),                     # KPI Spark = volume / market_cap
    Column("dominance_per_token", Float),                            # KPI Spark = market_cap / num_token
    Column("market_cap_change_rate", Float),                         # KPI Spark = market_cap_change / market_cap
    Column("volume_change_rate", Float),                             # KPI Spark = volume_change / volume
    Column("price_change_index", Float),                             # KPI Spark = avg_price_change * num_tokens

    PrimaryKeyConstraint("category_id", "snapshot_timestamp")
)

#|--------------------------------------------------------------------------------------------|
#|                                      DOMAIN: GLOBAL METRICS                                |
#|--------------------------------------------------------------------------------------------|

# Fact table: Snapshot-based global market indicators (dominance, volume, market cap) over time
fact_global_market = Table(
    "fact_global_market", metadata,

    # Snapshot identifiant
    Column("snapshot_timestamp", DateTime),                             # Spark -> to_timestamp
    Column("snapshot_str", String, nullable=False),                     # Spark -> date_format lisible
    Column("last_updated_timestamp", DateTime),                         # /global-metrics/quotes/latest -> to_timestamp (Spark)
    Column("last_updated_str", String),                                 # /global-metrics/quotes/latest -> date_format with time (Spark)

    # Dominance BTC / ETH
    Column("btc_dominance", Float),                                     # /global-metrics/quotes/latest -> btc_dominance   
    Column("eth_dominance", Float),                                     # /global-metrics/quotes/latest -> eth_dominance  
    Column("btc_dominance_yesterday", Float),                           # /global-metrics/quotes/latest -> btc_dominance_yesterday  
    Column("eth_dominance_yesterday", Float),                           # /global-metrics/quotes/latest -> eth_dominance_yesterday  
    Column("btc_dominance_24h_change", Float),                          # /global-metrics/quotes/latest -> btc_dominance_24h_change  
    Column("eth_dominance_24h_change", Float),                          # /global-metrics/quotes/latest -> eth_dominance_24h_change  
    Column("btc_dominance_delta", Float),                               # KPI Spark = btc_dominance - btc_dominance_yesterday
    Column("eth_dominance_delta", Float),                               # KPI Spark = eth_dominance - eth_dominance_yesterday
    Column("check_btc_dominance_delta", Boolean),                       # KPI Spark = True when btc_dominance_delta != 0.0 null else False 
    Column("check_eth_dominance_delta", Boolean),                       # KPI Spark = True when eth_dominance_delta != 0.0 else False 

    # General counts
    Column("active_cryptocurrencies", Integer),                         # /global-metrics/quotes/latest -> active_cryptocurrencies  
    Column("total_cryptocurrencies", Integer),                          # /global-metrics/quotes/latest -> total_cryptocurrencies  
    Column("active_market_pairs", Integer),                             # /global-metrics/quotes/latest -> active_market_pairs  
    Column("active_exchanges", Integer),                                # /global-metrics/quotes/latest -> active_exchanges  
    Column("total_exchanges", Integer),                                 # /global-metrics/quotes/latest -> total_exchanges  

    # DeFi metrics
    Column("defi_market_cap", Float),                                   # /global-metrics/quotes/latest -> defi_market_cap  
    Column("defi_volume_24h", Float),                                   # /global-metrics/quotes/latest -> defi_volume_24h  
    Column("defi_volume_24h_reported", Float),                          # /global-metrics/quotes/latest -> defi_volume_24h_reported  
    Column("defi_24h_percentage_change", Float),                        # /global-metrics/quotes/latest -> defi_24h_percentage_change  
    Column("defi_volume_share", Float),                                 # KPI Spark = defi_volume_24h / total_volume_24h
    Column("check_defi_volume_share", Boolean),                         # KPI Spark = True when defi_volume_share != 0.0 else False 

    # Stablecoins metrics
    Column("stablecoin_market_cap", Float),                             # /global-metrics/quotes/latest -> stablecoin_market_cap  
    Column("stablecoin_volume_24h", Float),                             # /global-metrics/quotes/latest -> stablecoin_volume_24h  
    Column("stablecoin_volume_24h_reported", Float),                    # /global-metrics/quotes/latest -> stablecoin_volume_24h_reported  
    Column("stablecoin_24h_percentage_change", Float),                  # /global-metrics/quotes/latest -> stablecoin_24h_percentage_change  
    Column("stablecoin_market_share", Float),                           # KPI Spark = stablecoin_market_cap / total_market_cap
    Column("check_stablecoin_market_share", Boolean),                   # KPI Spark = True when stablecoin_market_share != 0.0 else False 

    # Derivatives
    Column("derivatives_volume_24h", Float),                            # /global-metrics/quotes/latest -> derivatives_volume_24h  
    Column("derivatives_volume_24h_reported", Float),                   # /global-metrics/quotes/latest -> derivatives_volume_24h_reported  
    Column("derivatives_24h_percentage_change", Float),                 # /global-metrics/quotes/latest -> derivatives_24h_percentage_change  

    # Altcoins
    Column("altcoin_market_cap", Float),                                # /global-metrics/quotes/latest -> altcoin_market_cap
    Column("altcoin_volume_24h", Float),                                # /global-metrics/quotes/latest -> altcoin_volume_24h  
    Column("altcoin_volume_24h_reported", Float),                       # /global-metrics/quotes/latest -> altcoin_volume_24h_reported  

    # Global market
    Column("total_market_cap", Float),                                  # /global-metrics/quotes/latest -> total_market_cap
    Column("total_volume_24h", Float),                                  # /global-metrics/quotes/latest -> total_volume_24h
    Column("total_volume_24h_reported", Float),                         # /global-metrics/quotes/latest -> total_volume_24h_reported
    Column("market_liquidity_ratio", Float),                            # KPI Spark = total_volume_24h / total_market_cap
    Column("check_market_liquidity_ratio", Boolean),                    # KPI Spark = True when market_liquidity_ratio != 0.0 else False
    Column("total_market_cap_yesterday", Float),                        # /global-metrics/quotes/latest -> total_market_cap_yesterday
    Column("total_volume_24h_yesterday", Float),                        # /global-metrics/quotes/latest -> total_volume_24h_yesterday
    Column("total_market_cap_yesterday_change", Float),                 # /global-metrics/quotes/latest -> total_market_cap_yesterday_change
    Column("total_volume_24h_yesterday_change", Float),                 # /global-metrics/quotes/latest -> total_volume_24h_yesterday_change

    # New crypto creation (growth)
    Column("total_crypto_dex_currencies", Integer),                     # /global-metrics/quotes/latest -> total_crypto_dex_currencies
    Column("new_cryptos_today", Integer),                               # /global-metrics/quotes/latest -> today_incremental_crypto_number
    Column("new_cryptos_last_24h", Integer),                            # /global-metrics/quotes/latest -> past_24h_incremental_crypto_number
    Column("new_cryptos_last_7d", Integer),                             # /global-metrics/quotes/latest -> past_7d_incremental_crypto_number
    Column("new_cryptos_last_30d", Integer),                            # /global-metrics/quotes/latest -> past_30d_incremental_crypto_number
    Column("new_cryptos_today_pct", Float),                             # /global-metrics/quotes/latest -> today_change_percent
    Column("crypto_growth_rate_30d", Float),                            # KPI Spark = new_cryptos_last_30d / total_cryptocurrencies
    Column("check_crypto_growth_rate_30d", Boolean),                    # KPI Spark = true when crypto_growth_rate_30d != 0.0 else Flase
    Column("max_daily_new_cryptos", Integer),                           # /global-metrics/quotes/latest -> tracked_maxIncrementalNumber
    Column("max_new_cryptos_day", Date),                                # /global-metrics/quotes/latest -> tracked_maxIncrementalDate
    Column("min_daily_new_cryptos", Integer),                           # /global-metrics/quotes/latest -> tracked_minIncrementalNumber
    Column("min_new_cryptos_day", Date),                                # /global-metrics/quotes/latest -> tracked_minIncrementalDate

    PrimaryKeyConstraint("snapshot_timestamp")
)

# -------------------------------------------------------------------------------------------------
# The `check_*` attributes serve as boolean flags indicating whether each calculated KPI
# (e.g., dominance_delta, liquidity_ratio, volume_share) has a meaningful value.
# They are set to True when the corresponding KPI is non-zero and valid,
# allowing downstream tools (SQL, BI, ML) to easily filter out placeholder or default values.
# This improves the reliability of analytics and avoids misleading zero values.
# -------------------------------------------------------------------------------------------------


#|--------------------------------------------------------------------------------------------|
#|                                      DOMAIN: FEAR & GREED                                  | 
#|--------------------------------------------------------------------------------------------|
