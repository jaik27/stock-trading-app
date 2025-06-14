#from socketclusterclient import Socketcluster
#mport Socketcluster  #using modified socketcluster in current dir, instead of the one installed pip(file SocketCluster.py in current dir)
import logging
import requests 
import pandas as pd
        
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
        
        
def onconnect(socket):
    logging.info("on connect got called")


def ondisconnect(socket):
    logging.info("on disconnect got called")


def onConnectError(socket, error):
    logging.info("On connect error got called")


def onSetAuthentication(socket, token):
    logging.info("Token received " + token)
    socket.setAuthtoken(token)


def onAuthentication(socket, isauthenticated):
    logging.info("Authenticated is " + str(isauthenticated))
    #subscribe_to_channel(socket,'yell')
    subscribe_to_first_five_tickers(socket)

def suback(channel, error, object):
    if error is '':
        print ("Subscribed successfully to channel " , channel)

def unsuback(channel, error, object):
    if error is '':
       print ("Unsubscribed to channel " , channel)

def puback(channel, error, object):
    if error is '':
        logging.info("Publish sent successfully to channel " + channel)

def ack(key, error, object):
    logging.info("Got ack data " + object + " and error " + error + " and key is " + key)

def message(key, object):
    logging.info("Got data " + object + " from key " + key)


def messsageack(key, object, ackmessage):
    logging.info("Got data " + object + " from key " + key)
    ackmessage("this is error", "this is data")

def pong(key, object, ackmessage):
    logging.info("Got data " + object + " from key " + key)
    ackmessage("", "#2")

        

def channelmessage(key, object):
    logging.info("Got channel data " + object + " from key " + key)


#############################################################################################


def get_auth_token(loginId, product, apikey):
    authEndPoint = "http://s3.vbiz.in/directrt/gettoken?loginid={}&product={}&apikey={}".format(loginId, product, apikey)
    try:
        res = requests.get(authEndPoint)
        if res.status_code == 200:
            return True, 'Access token returned successfully', res.json()['AccessToken']
        else:
            return False, 'Response code returned not 200', res.text
    except Exception as e:
        return False, 'Network error occurred trying to get access token', str(e)

def subscribe_to_channel(socket,channel):
    # without acknowledgement
    #socket.subscribe('yell')
    
    #with acknowledgement
    #socket.subscribeack('yell', suback) #suback is the function you want  to be called,when acknowledgement is received from server
    socket.subscribeack(channel, suback)


def unsubscribe_from_channel(socket,channel):
    # without acknowledgement
    #socket.unsubscribe('yell')
    
    # with acknowledgement
    #socket.unsubscribeack('yell', unsuback) #unsuback is the function you want  to be called,when acknowledgement is received from server
    socket.unsubscribeack(channel, unsuback)


def get_all_subscribed_channels():
    channels = socket.getsubscribedchannels()
    print('all subscribed_channels: ',channels)
    return True,channels

def download_tickers(loginId,product,access_token):
    try:
        #gettickers = "gettickers?loginid={loginid}&product={product}&accesstoken={accesstkn}"
        reqEndPoint = "http://qbase1.vbiz.in/directrt/"
        #url = reqEndPoint + addurlqueries(gettickers)
        url = "http://qbase1.vbiz.in/directrt/gettickers?loginid={}&product={}&accesstoken={}".format(loginId,product,access_token)
        res = requests.get(url)
        if not res.status_code == 200:
            #error occured getting the tickers
            return False, 'Error occured getting tickers',res.status_code
        else:
            #write the tickers to file(overwrite)
            with open('tickers.txt','w') as f:
                f.write(res.text)
                print('tickers downloaded, saved to file `tickers.txt`')
    except Exception as e:
        print('Exception occured while trying to get tickers: ',str(e))
        return False,'Exception occured while trying to get tickers',str(e)

def subscribe_to_first_five_tickers(socket):
    nse_tickers = ['NSE_OPTIDX_NIFTY_05JUN2025_24700_CE.csv',
'NSE_OPTIDX_NIFTY_05JUN2025_24700_CE.json']


    for ticker in nse_tickers:
        subscribe_to_channel(socket,ticker)
        socket.onchannel(ticker, channelmessage)


def get_historical_data(
    loginId, product, access_token, startdate, enddate,
    exch, inst, symbol, expiry=None, strike=None, optiontype=None
):
    """
    Fetch historical data from the API.
    Dates should be in format: DDMMMYYYY (e.g., 12FEB2025)
    For options, provide expiry, strike, and optiontype.
    """
    url = (
        "http://qbase1.vbiz.in/directrt/gethistorical"
        "?loginid={}&product={}&accesstoken={}&startdate={}&enddate={}&exch={}&inst={}&symbol={}"
    ).format(loginId, product, access_token, startdate, enddate, exch, inst, symbol)
    if expiry:
        url += f"&expiry={expiry}"
    if strike:
        url += f"&strike={strike}"
    if optiontype:
        url += f"&optiontype={optiontype}"
    try:
        res = requests.get(url)
        if res.status_code == 200:
            print("Historical data fetched successfully.")
            try:
                # Process the response as CSV
                from io import StringIO
                csv_data = StringIO(res.text)
                df = pd.read_csv(csv_data)
                return True, df
            except Exception as e:
                print("Error processing CSV data:", str(e))
                return False, res.text
        else:
            print("Failed to fetch historical data. Status code:", res.status_code)
            return False, res.text
    except Exception as e:
        print("Exception occurred while fetching historical data:", str(e))
        return False, str(e)

def main():

    loginId = 'DC-BPRA9112'  # Replace with your login ID
    product = 'DIRECTRTLITE'  # Replace with your product
    apikey = 'B4D2ECAFA32941AC93ED'  # Replace with your API key

    # Get access token
    res = get_auth_token(loginId, product, apikey)
    if not res[0]:
        print(res[1], res[2])
        return

    # Access token returned successfully, if execution reaches here.
    access_token = res[2]
    # print('access token returned: ', access_token)

    # Get tickers
    #download_tickers(loginId, product, access_token)

    # Example: Fetch historical data and export to CSV
    # For options, split symbol into symbol, expiry, strike, optiontype
    success, data = get_historical_data(
        loginId, product, access_token,
        "24JUN2021", "24JUN2021", "NSE", "OPTIDX",
        "NIFTY", "24JUN2021", "15000", "CE"
    )
    if success:
        # Export DataFrame to CSV
        output_file = input("Enter the output CSV file namTeste (default: historical_data.csv): ").strip()
        if not output_file:
            output_file = "historical_data.csv"

        try:
            data.to_csv(output_file, index=False)
            print(f"Historical data exported to {output_file}")
            print(data.describe(include='all'))
            print("\nFirst row:")
            print(data.iloc[0])
            print("\nLast row:")
            print(data.iloc[-1])
        except Exception as e:
            print(f"Error exporting data to {output_file}: {e}")
    else:
        print("Failed to fetch historical data:", data)

    # Connect to websocket
    wsEndPoint = "ws://116.202.165.216:992/directrt/?loginid={}&accesstoken={}&product={}".format(loginId, access_token, product)
    # print('final websocket endpoint to call: ', wsEndPoint)
    # socket = Socketcluster.socket(wsEndPoint)
    # socket.setBasicListener(onconnect, ondisconnect, onConnectError)
    # socket.setAuthenticationListener(onSetAuthentication, onAuthentication)
    # socket.onack('ping', pong)  # listening to event
    # socket.on('unsubscribed', message)  # listening to event
    # socket.on('subscribed', message)  # listening to event
    # socket.onchannel('yell', channelmessage)  # listening to channel
    # socket.enablelogger(True)
    # socket.connect()


if __name__ == '__main__':
    main()