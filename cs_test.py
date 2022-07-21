import pandas as pd
from subgrounds.subgrounds import Subgrounds
import numpy as np
import datetime as dt
import asyncio
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode 
from aiocache import Cache
from aiocache import cached
from refresh_component import refresh_component

sg = Subgrounds()
headers = ['Protocol', 'Chain', 'TVL ($)', 'Realized Losses ($)', 'At-Risk Capital ($)', 'At-Risk Capital (%)']

    
def get_tvl(endpoint):
    market_tvl_data = endpoint.Query.marketDailySnapshots(first=1000, orderBy=endpoint.MarketDailySnapshot.timestamp, orderDirection='desc')
    df = sg.query_df([
        market_tvl_data.timestamp,
        market_tvl_data.market.name,
        market_tvl_data.totalValueLockedUSD
    ])
    
    df['marketDailySnapshots_timestamp'] = pd.to_datetime(df['marketDailySnapshots_timestamp'], unit='s').dt.strftime('%Y-%m-%d')
    time_variable = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    df = df.loc[df['marketDailySnapshots_timestamp'] == time_variable]
    df = df.rename(columns={'marketDailySnapshots_timestamp':'Timestamp', 'marketDailySnapshots_market_name':'Market', 'marketDailySnapshots_totalValueLockedUSD':'TVL'})
    df['TVL'] = df['TVL'].round(0)
    agg_tvl = df['TVL'].sum().round(0)
    return df, agg_tvl

#get protocol deposits
def get_deposits(endpoint):
    deposit_data = endpoint.Query.positions(first=1000000, where=[endpoint.Position.side == 'LENDER', endpoint.Position.isCollateral == True, endpoint.Position.balance > 0])
    df = sg.query_df([
        deposit_data.account.id,
        deposit_data.side,
        deposit_data.isCollateral,
        deposit_data.balance,
        deposit_data.market.name,
        deposit_data.market.maximumLTV,
        deposit_data.market.liquidationThreshold,
        deposit_data.market.inputToken.symbol,
        deposit_data.market.inputToken.decimals
     ])

    return df

#get protocol borrows
def get_borrows(endpoint):
    borrow_data = endpoint.Query.positions(first=1000000, where=[endpoint.Position.side == 'BORROWER', endpoint.Position.balance > 0])
    df = sg.query_df([
        borrow_data.account.id,
        borrow_data.side,
        borrow_data.isCollateral,
        borrow_data.balance,
        borrow_data.market.name,
        borrow_data.market.maximumLTV,
        borrow_data.market.liquidationThreshold,
        borrow_data.market.inputToken.symbol,
        borrow_data.market.inputToken.decimals
     ])
    return df

#get asset prices on protocol
def get_prices(endpoint):
    price_data = endpoint.Query.marketDailySnapshots(first=100, orderBy=endpoint.Query.marketDailySnapshots.timestamp, orderDirection='desc')
    df = sg.query_df([
        price_data.timestamp,
        price_data.market.name,
        price_data.inputTokenPriceUSD
    ])
    
    df['marketDailySnapshots_timestamp'] = df['marketDailySnapshots_timestamp'].astype(int)
    df['marketDailySnapshots_timestamp'] = pd.to_datetime(df['marketDailySnapshots_timestamp'], unit='s').dt.strftime('%Y-%m-%d')
    df = df.loc[df['marketDailySnapshots_timestamp'] == dt.datetime.now().strftime('%Y-%m-%d')]
    
    return df

#get protocol collateral deposits where == 0 (ie: all collateral fully liquidated)
def get_liquidated_deposit_balances(endpoint):
    deposit_data = endpoint.Query.positions(first=1000000, where=[endpoint.Position.side == 'LENDER', endpoint.Position.isCollateral == True, endpoint.Position.balance == 0])
    df = sg.query_df([
        deposit_data.account.id,
        deposit_data.side,
        deposit_data.isCollateral,
        deposit_data.balance,
        deposit_data.market.name,
        deposit_data.market.maximumLTV,
        deposit_data.market.liquidationThreshold,
        deposit_data.market.inputToken.symbol,
        deposit_data.market.inputToken.decimals
     ])

    return df


def realized_losses(endpoint, price_df, liquidated_deposits_df, borrows_df):

    #merge deposits and borrows, clean, format
    df = pd.merge(liquidated_deposits_df, borrows_df, how='left', on=['positions_account_id', 'positions_market_name'])
    df['positions_balance_y'] = df['positions_balance_y'].fillna(0)
    df['positions_balance_x'] = df['positions_balance_x'] / np.power(10, df['positions_market_inputToken_decimals_x'])
    df['positions_balance_y'] = df['positions_balance_y'] / np.power(10, df['positions_market_inputToken_decimals_x'])

    #merge price df w/ deposits/borrows df
    final_df = pd.merge(df, price_df, left_on=['positions_market_name'], right_on=['marketDailySnapshots_market_name'])
    final_df = final_df.drop(['marketDailySnapshots_market_name', 'marketDailySnapshots_timestamp', 'positions_side_y', 'positions_side_x', 'positions_isCollateral_x', 'positions_market_maximumLTV_x', 'positions_isCollateral_y', 'positions_market_maximumLTV_y', 'positions_market_liquidationThreshold_y', 'positions_market_inputToken_symbol_y', 'positions_market_inputToken_decimals_y'], axis=1)
    final_df = final_df.rename(columns={'positions_account_id':'User Address', 'positions_balance_x':'Deposit Balance', 'positions_market_name':'Market', 'positions_market_liquidationThreshold_x':'Liquidation Threshold', 'positions_market_inputToken_symbol_x':'Token Symbol', 'positions_market_inputToken_decimals_x':'Decimals', 'positions_balance_y':'Borrows Balance', 'marketDailySnapshots_inputTokenPriceUSD':'Token Price'})
    final_df['Deposit Balance USD'] = final_df['Token Price'] * final_df['Deposit Balance']
    final_df['Borrows Balance USD'] = final_df['Token Price'] * final_df['Borrows Balance']
    
    #group by market, then I can save this as a variable and get agg values off of this as well
    losses_per_market = final_df.groupby(['Market'])[['Deposit Balance USD', 'Borrows Balance USD']].apply(lambda x : x.astype(int).sum()).reset_index()
    losses_per_market = losses_per_market.rename(columns={'Borrows Balance USD':'Realized Losses'})
    losses_per_market = losses_per_market.drop(['Deposit Balance USD'], axis=1)
    agg_losses = losses_per_market['Realized Losses'].sum()

    return(losses_per_market, agg_losses)

#compile df, leave as per market -> following function will get per asset metrics and agg metrics
def deposits_borrows_df(endpoint, price_df, deposits_df, borrows_df):

    df = pd.merge(deposits_df, borrows_df, how='left', on=['positions_account_id', 'positions_market_name'])
    df['positions_balance_y'] = df['positions_balance_y'].fillna(0)
    df['positions_balance_x'] = df['positions_balance_x'] / np.power(10, df['positions_market_inputToken_decimals_x'])
    df['positions_balance_y'] = df['positions_balance_y'] / np.power(10, df['positions_market_inputToken_decimals_x'])
    #merge price df w/ deposits/borrows df
    final_df = pd.merge(df, price_df, left_on=['positions_market_name'], right_on=['marketDailySnapshots_market_name'])
    final_df = final_df.drop(['marketDailySnapshots_market_name', 'marketDailySnapshots_timestamp', 'positions_side_y', 'positions_side_x', 'positions_isCollateral_x', 'positions_market_maximumLTV_x', 'positions_isCollateral_y', 'positions_market_maximumLTV_y', 'positions_market_liquidationThreshold_y', 'positions_market_inputToken_symbol_y', 'positions_market_inputToken_decimals_y'], axis=1)

    final_df = final_df.rename(columns={'positions_account_id':'User Address', 'positions_balance_x':'Deposit Balance', 'positions_market_name':'Market', 'positions_market_liquidationThreshold_x':'Liquidation Threshold', 'positions_market_inputToken_symbol_x':'Token Symbol', 'positions_market_inputToken_decimals_x':'Decimals', 'positions_balance_y':'Borrows Balance', 'marketDailySnapshots_inputTokenPriceUSD':'Token Price'})
    final_df['Deposit Balance USD'] = final_df['Token Price'] * final_df['Deposit Balance']
    final_df['Borrows Balance USD'] = final_df['Token Price'] * final_df['Borrows Balance']

    return(final_df)


#gather data for protocols
def get_protocol_data(protocol, chain, url):
    #create endpoint 
    endpoint = sg.load_subgraph(url)
    
    #get all data
    market_tvl, agg_tvl = get_tvl(endpoint)
    
    borrows_df = get_borrows(endpoint)
    deposits_df = get_deposits(endpoint)
    liquidated_deposits_df = get_liquidated_deposit_balances(endpoint)
    price_df = get_prices(endpoint)
    depos_borrows_df = deposits_borrows_df(endpoint, price_df, deposits_df, borrows_df)
    losses_per_market, agg_losses = realized_losses(endpoint, price_df, liquidated_deposits_df, borrows_df)
    output = [protocol, chain, market_tvl, agg_tvl, depos_borrows_df, losses_per_market, agg_losses]
    
    return output

#async version of get_protocol_data
async def async_get_protocol_data(protocol, chain, url):
    return asyncio.to_thread(get_protocol_data, protocol, chain, url)


#run all data gathering asynchronously. Add additional subgraphs here

@cached(ttl=None, cache=Cache.MEMORY)
async def main():
    arbitrum_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-arbitrum-extended'
    optimism_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-optimism-extended'
    fantom_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-fantom-extended'
    harmony_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-harmony-extended'
    avalanche_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-avalanche-extended'
    result = await asyncio.gather(*[
        await async_get_protocol_data('AAVE', 'Harmony', harmony_url),
        await async_get_protocol_data('AAVE', 'Avalanche', avalanche_url),
        await async_get_protocol_data('AAVE', 'Arbitrum', arbitrum_url),
        await async_get_protocol_data('AAVE', 'Fantom', fantom_url),
        await async_get_protocol_data('AAVE', 'Optimism', optimism_url)
        ])
    aave_harmony = result[0]
    aave_avalanche = result[1]
    aave_arbitrum = result[2]
    aave_fantom = result[3]
    aave_optimism = result[4]

    return aave_harmony, aave_avalanche, aave_arbitrum, aave_fantom, aave_optimism


###Non-async version of main()
# @st.cache(allow_output_mutation=True)
# def main():
#     arbitrum_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-arbitrum-extended'
#     optimism_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-optimism-extended'
#     fantom_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-fantom-extended'
#     harmony_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-harmony-extended'
#     avalanche_url = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-avalanche-extended'
#     result = [
#     get_protocol_data('AAVE', 'Harmony', harmony_url),
#     get_protocol_data('AAVE', 'Avalanche', avalanche_url),
#     get_protocol_data('AAVE', 'Arbitrum', arbitrum_url),
#     get_protocol_data('AAVE', 'Fantom', fantom_url),
#     get_protocol_data('AAVE', 'Optimism', optimism_url)
#     ]

#     aave_harmony = result[0]
#     aave_avalanche = result[1]
#     aave_arbitrum = result[2]
#     aave_fantom = result[3]
#     aave_optimism = result[4]

#     return aave_harmony, aave_avalanche, aave_arbitrum, aave_fantom, aave_optimism
#print(result)


#get values at risk
def get_var(x, discount_factor):
    data = x
    data_df = pd.DataFrame(data)
    data_df['Discounted Deposit Balance USD'] = data_df['Deposit Balance USD'] * discount_factor
    data_df['Max Borrowable Amount'] = data_df['Discounted Deposit Balance USD'] * (data_df['Liquidation Threshold'] / 100)
    data_df['Max Borrowable Amount'] = data_df['Max Borrowable Amount'].astype(int)
    data_df['Borrows Balance USD'] = data_df['Borrows Balance USD'].astype(int)
    data_df['Discounted Deposit Balance USD'] = data_df['Discounted Deposit Balance USD'].astype(int)

    data_df.loc[data_df['Borrows Balance USD'] > data_df['Max Borrowable Amount'], 'Value At Risk ($)'] = data_df['Borrows Balance USD'] - data_df['Max Borrowable Amount']
    data_df['Value At Risk ($)'] = data_df['Value At Risk ($)'].fillna(0)
    data_df = data_df[data_df['Max Borrowable Amount'] > 0]
    data_df = data_df.groupby(['User Address', 'Market'])[['Value At Risk ($)', 'Discounted Deposit Balance USD']].sum().reset_index()
    data_df = data_df.groupby(['Market'])[['Value At Risk ($)', 'Discounted Deposit Balance USD']].sum().reset_index()
    data_df['Value At Risk (%)'] = (data_df['Value At Risk ($)'] / data_df['Discounted Deposit Balance USD']) 
    per_market_var = data_df
    
    agg_var_usd = data_df['Value At Risk ($)'].sum()
    agg_var_perc = (data_df['Value At Risk ($)'].sum() / data_df['Discounted Deposit Balance USD'].sum()) * 100
    agg_var_perc = agg_var_perc.round(2)
    agg_var = [agg_var_usd, agg_var_perc]
    
    return (per_market_var, agg_var)

#compile value at risk data and output final formatted/styled dataframes
def compile_all(x, discount_factor):
    protocol = x[0]
    chain = x[1]
    market_tvl = x[2]
    agg_tvl = x[3]
    deposits_borrows = x[4]
    market_var, agg_var = get_var(deposits_borrows, discount_factor)
    #display(market_var)
    market_losses = x[5]
    agg_losses = x[6]
    agg_var_usd = agg_var[0]
    agg_var_perc = agg_var[1]
    final_df = pd.merge(market_tvl, market_losses, how='left', on='Market')
    final_df['Realized Losses'] = final_df['Realized Losses'].fillna(0)
    final_df = pd.merge(final_df, market_var, how='left', on='Market')
    final_df = final_df.drop(['Timestamp', 'Discounted Deposit Balance USD'], axis=1)
    final_df['Value At Risk ($)'] = final_df['Value At Risk ($)'].fillna(0)
    final_df['Value At Risk (%)'] = final_df['Value At Risk (%)'].fillna(0)
    final_df['Value At Risk (%)'] = (final_df['Value At Risk (%)'] * 100).round(2)
    final_df = final_df.rename(columns={'TVL':'TVL ($)', 'Realized Losses':'Realized Losses ($)'})
    final_df = final_df.reset_index(drop=True)
    #final_df = final_df.style.format({'Value At Risk ($)': '${0:,.0f}', 'TVL':'${0:,.0f}', \
    #                                 'Realized Losses': '${0:,.0f}', 'Value At Risk (%)': '{0:,.2%}'})
    final_list = [protocol, chain, agg_tvl, agg_losses, agg_var_usd, agg_var_perc]


    return final_df, final_list





##NEW CODE
def aggrid_interactive_table(df):
    options = GridOptionsBuilder.from_dataframe(
        df, 
        enableRowGroup=True,
        enableValue=True
        #enablePivot=True
        )

    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme='dark',
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
        height=300
        )
    return selection
###############





#aave_harmony, aave_avalanche, aave_arbitrum, aave_fantom, aave_optimism = main()
aave_harmony, aave_avalanche, aave_arbitrum, aave_fantom, aave_optimism = asyncio.run(main())



st.title("Credit Score App")



###variables & get_protocol_data objects:
discount = st.selectbox('If Collateral Value falls x%: ', [-5, -10, -15, -20, -25, -50])
discount_factor = 1 - (abs(discount)/100)


# if 'discount_variable' not in st.session_state:
#     st.session_state['discount_variable'] = discount_factor
# st.session_state['discount_variable'] = discount_factor
# st.write(st.session_state['discount_variable'])



aave_harmony_data = compile_all(aave_harmony, discount_factor)
aave_avalanche_data = compile_all(aave_avalanche, discount_factor)
aave_arbitrum_data = compile_all(aave_arbitrum, discount_factor)
aave_fantom_data = compile_all(aave_fantom, discount_factor)
aave_optimism_data = compile_all(aave_optimism, discount_factor)


aave_harmony_markets = aave_optimism_data[0]
aave_avalanche_markets = aave_avalanche_data[0]
aave_arbitrum_markets = aave_arbitrum_data[0]
aave_fantom_markets = aave_fantom_data[0]
aave_optimism_markets = aave_optimism_data[0]



comprehensive_list = [aave_harmony_data[1], aave_avalanche_data[1], aave_arbitrum_data[1], aave_fantom_data[1], aave_optimism_data[1]]
agg_df = pd.DataFrame(comprehensive_list)
agg_df.columns = headers
agg_df = agg_df.reset_index(drop=True)
#agg_df = agg_df.style.format({'TVL': '${0:,.0f}', 'Realized Losses': '${0:,.0f}', 'At-Risk Capital ($)': '${0:,.0f}', 'At-Risk Capital (%)': '{0:,.2%}'})
#st.write(agg_df)





chain_dict = {'Harmony':aave_harmony_markets, 'Avalanche':aave_avalanche_markets, 'Arbitrum':aave_arbitrum_markets, 'Fantom':aave_fantom_markets, 'Optimism':aave_optimism_markets}

placeholder = st.empty()
try:
    with placeholder.container():
        st.subheader('Aggregate Data')
        selection = aggrid_interactive_table(agg_df)
        if selection:
            selected_chain = selection["selected_rows"][0]['Chain']
            if selected_chain:
                placeholder = placeholder.empty()
                with placeholder.container():
                    st.subheader('Per Pool')
                    new_selection = chain_dict[selected_chain]
                    new_df = aggrid_interactive_table(new_selection)

  


except:
    pass


if st.button('Back'):
    refresh_component()


