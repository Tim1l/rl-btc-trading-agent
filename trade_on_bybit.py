import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import logging
import asyncio
import telegram

# Настройки
SYMBOL = "BTCUSDT"  # BTC/USDT perpetual
ACCOUNTS_FILE = "bybit_account.json"
TELEGRAM_TOKEN = ""  # Замени на твой токен
TELEGRAM_CHANNEL = ""  # Замени на твой ID канала

# Логирование
logging.basicConfig(
    filename="bybit_trading.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

def read_last_action(last_processed_step, start_step=961):
    """Читает все необработанные действия из rl_actions_history.csv начиная с max(last_processed_step, start_step-1)."""
    try:
        df = pd.read_csv("rl_actions_history.csv")
        # Определяем начальный шаг: для нового запуска игнорируем шаги до start_step-1
        effective_start_step = max(last_processed_step, start_step - 1)
        pending_actions = df[df["step"] > effective_start_step][["step", "date", "action", "position"]]
        if pending_actions.empty:
            logging.info(f"No new actions after step {effective_start_step}")
            return []
        logging.info(f"Found {len(pending_actions)} pending actions after step {effective_start_step}")
        return pending_actions.to_dict("records")
    except Exception as e:
        logging.error(f"Failed to read rl_actions_history.csv: {e}")
        return []

def sign_request(api_key, api_secret, timestamp, recv_window, params):
    param_str = f"{timestamp}{api_key}{recv_window}{params}"
    hash = hmac.new(api_secret.encode("utf-8"), param_str.encode("utf-8"), hashlib.sha256)
    return hash.hexdigest()

async def get_current_price(api_key, api_secret, symbol):
    """Получает текущую цену символа через recent-trade."""
    try:
        url = "https://api-demo.bybit.com/v5/market/recent-trade"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        params = f"category=linear&symbol={symbol}"
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window
        }

        response = requests.get(url, headers=headers, params={"category": "linear", "symbol": symbol})
        logging.info(f"Ticker HTTP status: {response.status_code}")
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to get price: {data['retMsg']}")
            return None
        price = float(data["result"]["list"][0]["price"])
        logging.info(f"Current price: {price}")
        return price
    except Exception as e:
        logging.error(f"Failed to get price: {e}")
        return None

async def get_bybit_position(api_key, api_secret, symbol):
    """Получает текущую позицию на Bybit."""
    try:
        url = "https://api-demo.bybit.com/v5/position/list"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        params = f"category=linear&symbol={symbol}"
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window
        }

        response = requests.get(url, headers=headers, params={"category": "linear", "symbol": symbol})
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to get position: {data['retMsg']}")
            return None

        for pos in data["result"]["list"]:
            if pos["symbol"] == symbol:
                size = float(pos["size"])
                side = pos["side"].lower()
                if size == 0:
                    return 0
                return 1 if side == "buy" else -1
        return 0
    except Exception as e:
        logging.error(f"Failed to get position: {e}")
        return None

async def get_bybit_balance(api_key, api_secret):
    """Получает общий маржинальный баланс на Bybit."""
    try:
        url = "https://api-demo.bybit.com/v5/account/wallet-balance"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        params = "accountType=UNIFIED"
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window
        }

        response = requests.get(url, headers=headers, params={"accountType": "UNIFIED"})
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to get balance: {data['retMsg']}")
            return None

        total_margin_balance = float(data["result"]["list"][0]["totalMarginBalance"])
        coins = data["result"]["list"][0]["coin"]
        logging.info(f"Available coins: {[coin['coin'] + ': ' + coin['equity'] for coin in coins]}")
        logging.info(f"Total margin balance: {total_margin_balance}")
        return total_margin_balance
    except Exception as e:
        logging.error(f"Failed to get balance: {e}")
        return None

async def cancel_stop_loss(api_key, api_secret, symbol):
    """Отменяет все стоп-ордера для символа."""
    try:
        url = "https://api-demo.bybit.com/v5/order/cancel-all"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        params = {
            "category": "linear",
            "symbol": symbol
        }
        params_str = json.dumps(params, separators=(',', ':'))
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params_str)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, data=params_str)
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to cancel stop-loss: {data['retMsg']}")
            return False
        logging.info(f"Stop-loss orders cancelled for {symbol}")
        return True
    except Exception as e:
        logging.error(f"Failed to cancel stop-loss: {e}")
        return False

async def place_bybit_order(api_key, api_secret, symbol, side, amount, stop_loss_price):
    """Отправляет ордер и стоп-лосс на Bybit."""
    try:
        url = "https://api-demo.bybit.com/v5/order/create"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"

        # Основной ордер
        order_params = {
            "category": "linear",
            "symbol": symbol,
            "side": side.capitalize(),
            "orderType": "Market",
            "qty": str(amount),
            "timeInForce": "GTC"
        }
        params_str = json.dumps(order_params, separators=(',', ':'))
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params_str)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, data=params_str)
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to place order: {data['retMsg']}")
            return False
        logging.info(f"Order placed: {side} {amount} {symbol}")

        if stop_loss_price > 0:
            # Стоп-лосс
            stop_params = {
                "category": "linear",
                "symbol": symbol,
                "side": "Sell" if side == "buy" else "Buy",
                "orderType": "Limit",
                "qty": str(amount),
                "price": str(round(stop_loss_price, 2)),
                "triggerPrice": str(round(stop_loss_price, 2)),
                "triggerDirection": 2 if side == "buy" else 1,  # Falling для лонга, Rising для шорта
                "timeInForce": "GTC",
                "positionIdx": 0
            }
            logging.info(f"Stop-loss params: price={stop_loss_price}, triggerPrice={stop_loss_price}")
            stop_params_str = json.dumps(stop_params, separators=(',', ':'))
            stop_signature = sign_request(api_key, api_secret, timestamp, recv_window, stop_params_str)

            headers["X-BAPI-SIGN"] = stop_signature
            response = requests.post(url, headers=headers, data=stop_params_str)
            data = response.json()
            if data["retCode"] != 0:
                logging.error(f"Failed to place stop-loss: {data['retMsg']}")
                return False
            logging.info(f"Stop-loss placed: {stop_loss_price}")
        return True
    except Exception as e:
        logging.error(f"Failed to place order/stop-loss: {e}")
        return False

def read_accounts():
    """Читает bybit_account.json."""
    try:
        with open(ACCOUNTS_FILE, "r") as f:
            data = json.load(f)
            return data  # Возвращаем весь объект, включая last_update и account
    except Exception as e:
        logging.error(f"Failed to read {ACCOUNTS_FILE}: {e}")
        return None

def update_accounts(data):
    """Обновляет bybit_account.json."""
    try:
        with open(ACCOUNTS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Updated {ACCOUNTS_FILE}")
    except Exception as e:
        logging.error(f"Failed to update {ACCOUNTS_FILE}: {e}")

async def get_bybit_closed_pnl(api_key, api_secret, symbol):
    """Получает PNL последней закрытой позиции через API Bybit."""
    try:
        url = "https://api-demo.bybit.com/v5/position/closed-pnl"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        params = f"category=linear&symbol={symbol}"
        signature = sign_request(api_key, api_secret, timestamp, recv_window, params)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window
        }

        response = requests.get(url, headers=headers, params={"category": "linear", "symbol": symbol})
        logging.info(f"Closed PNL HTTP status: {response.status_code}")
        data = response.json()
        if data["retCode"] != 0:
            logging.error(f"Failed to get closed PNL: {data['retMsg']}")
            return None
        # Берем последнюю запись PNL
        # if not data["result"]["list"]:
        #     logging.error("No closed PNL records found")
        #     return None
        # closed_pnl = float(data["result"]["list"][0]["closedPnl"])
        # logging.info(f"Closed PNL: {closed_pnl}")
        # return closed_pnl
        # Берем последнюю запись PNL по времени закрытия
        if not data["result"]["list"]:
            logging.error("No closed PNL records found")
            return None
        # Сортируем по updatedTime (в миллисекундах) в порядке убывания
        latest_pnl = max(data["result"]["list"], key=lambda x: int(x["updatedTime"]))
        closed_pnl = float(latest_pnl["closedPnl"])
        logging.info(f"Closed PNL: {closed_pnl}, updatedTime: {latest_pnl['updatedTime']}")
        return closed_pnl
    except Exception as e:
        logging.error(f"Failed to get closed PNL: {e}")
        return None

async def send_log_to_telegram(action, balance, initial_balance, price, position_size, stop_loss, closed_pnl, warnings):
    """Отправляет краткий лог в Telegram-канал в человеческом формате."""
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        formatted_log = "📊 Bybit Trading Update 📊\n\n"
        formatted_log += f"💰 Balance: {balance} USDT\n"
        formatted_log += f"📈 BTCUSDT price: {price}\n" if price else ""
        if action == "Long" and position_size:
            formatted_log += f"🚀 Entered long position: {position_size} BTC\n"
        elif action == "Short" and position_size:
            formatted_log += f"📉 Entered short position: {position_size} BTC\n"
        elif action == "Close":
            formatted_log += f"✅ Position is closed\n"
            if closed_pnl is not None:
                formatted_log += f"📊 Trade result: {closed_pnl:.2f} USDT\n"
            else:
                formatted_log += "📊 Trade result: can't get it\n"
        # Отображаем стоп-лосс только если он не 0.0
        if stop_loss and stop_loss != "0.0":
            formatted_log += f"🛑 Stopp loss: {stop_loss}\n"
        if warnings:
            formatted_log += "\n⚠️ Warning:\n"
            for warn in warnings:
                formatted_log += f"- {warn}\n"

        max_length = 4000
        parts = [formatted_log[i:i+max_length] for i in range(0, len(formatted_log), max_length)]
        for i, part in enumerate(parts, 1):
            await bot.send_message(
                chat_id=TELEGRAM_CHANNEL,
                text=f"Bybit Account 1:\n{part}",
                parse_mode="HTML"
            )
        logging.info(f"Log sent to Telegram channel in {len(parts)} parts")
    except Exception as e:
        logging.error(f"Failed to send log to Telegram: {e}")

async def sync_bybit_account(data):
    """Получает позицию и баланс с Bybit, обрабатывает все необработанные действия начиная с шага 961."""
    try:
        # Получаем последний обработанный шаг, по умолчанию 0 для нового запуска
        last_processed_step = data["account"].get("last_processed_step", 0)
        start_step = 961  # Реальная торговля начинается с шага 961
        pending_actions = read_last_action(last_processed_step, start_step=start_step)
        
        # Баланс и позиция до действий
        initial_position = await get_bybit_position(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
        initial_balance = await get_bybit_balance(data["account"]["api_key"], data["account"]["api_secret"])
        if initial_position is None or initial_balance is None:
            logging.error(f"Failed to fetch position or balance for account {data['account']['id']}")
            return False, initial_position, initial_balance, False, None, []

        current_position = initial_position
        position_changed = False
        closed_pnl = None
        warnings = []

        # # Проверяем синхронизацию позиции с последним известным состоянием агента
        # nn_position = data["account"].get("last_update_position", 0)
        # if nn_position != initial_position:
        #     warning = f"Position mismatch: NN={nn_position}, Bybit={initial_position}"
        #     logging.warning(warning)
        #     warnings.append(warning)

        # Проверяем пропущенные шаги и берём только последнее действие
        if pending_actions:
            last_action_data = pending_actions[-1]  # Берём последнее действие
            step = last_action_data["step"]
            action = int(last_action_data["action"])
            nn_position = int(last_action_data["position"])
            action_date = last_action_data["date"]
            logging.info(f"Processing last step {step} (date {action_date}): action={action}, nn_position={nn_position}")

            # Проверяем пропущенные шаги
            if step > last_processed_step + 1:
                missed_steps = step - last_processed_step - 1
                warnings.append(f"Missed {missed_steps} steps before step {step}")

            # Пропускаем действия до start_step, если это первый запуск
            if step < start_step and last_processed_step == 0:
                logging.info(f"Skipping step {step} as it is before start_step {start_step}")
                data["account"]["last_processed_step"] = step
            else:
                # Открытие лонга
                if action == 0 and current_position == 0 and nn_position == 1:
                    current_price = await get_current_price(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                    if current_price is None:
                        logging.error(f"Failed to get price for step {step}")
                        warnings.append(f"Failed to get price for step {step}")
                    else:
                        position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                        position_size = round(max(position_size, 0.001), 3)
                        stop_loss_price = current_price * 0.9
                        logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                        if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "buy", position_size, stop_loss_price):
                            current_position = 1
                            data["account"]["position_size"] = position_size
                            data["account"]["stop_loss_price"] = stop_loss_price
                            position_changed = True
                        else:
                            logging.error(f"Failed to open long at step {step}")
                            warnings.append(f"Failed to open long at step {step}")

                # Открытие шорта
                elif action == 1 and current_position == 0 and nn_position == -1:
                    current_price = await get_current_price(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                    if current_price is None:
                        logging.error(f"Failed to get price for step {step}")
                        warnings.append(f"Failed to get price for step {step}")
                    else:
                        position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                        position_size = round(max(position_size, 0.001), 3)
                        stop_loss_price = current_price * 1.1
                        logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                        if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "sell", position_size, stop_loss_price):
                            current_position = -1
                            data["account"]["position_size"] = position_size
                            data["account"]["stop_loss_price"] = stop_loss_price
                            position_changed = True
                        else:
                            logging.error(f"Failed to open short at step {step}")
                            warnings.append(f"Failed to open short at step {step}")

                # Закрытие позиции
                elif (action == 0 or action == 1) and nn_position == 0 and current_position != 0:
                    position_size = data["account"].get("position_size", 0.0)
                    if position_size > 0:
                        if current_position == 1:  # Закрытие лонга
                            if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "sell", position_size, 0):
                                current_position = 0
                                data["account"]["position_size"] = 0.0
                                data["account"]["stop_loss_price"] = 0.0
                                await cancel_stop_loss(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                position_changed = True
                                await asyncio.sleep(20)
                                closed_pnl = await get_bybit_closed_pnl(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                if closed_pnl is not None:
                                    logging.info(f"Closed long at step {step}, PNL: {closed_pnl}")
                                else:
                                    warnings.append(f"Failed to get closed PNL at step {step}")
                            else:
                                logging.error(f"Failed to close long at step {step}")
                                warnings.append(f"Failed to close long at step {step}")
                        elif current_position == -1:  # Закрытие шорта
                            if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "buy", position_size, 0):
                                current_position = 0
                                data["account"]["position_size"] = 0.0
                                data["account"]["stop_loss_price"] = 0.0
                                await cancel_stop_loss(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                position_changed = True
                                await asyncio.sleep(20)
                                closed_pnl = await get_bybit_closed_pnl(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                if closed_pnl is not None:
                                    logging.info(f"Closed short at step {step}, PNL: {closed_pnl}")
                                else:
                                    warnings.append(f"Failed to get closed PNL at step {step}")
                            else:
                                logging.error(f"Failed to close short at step {step}")
                                warnings.append(f"Failed to close short at step {step}")

                # Синхронизация текущей позиции, если она отличается от nn_position
                elif current_position != nn_position:
                    if current_position != 0:  # Закрываем текущую позицию
                        position_size = data["account"].get("position_size", 0.0)
                        if position_size > 0:
                            if current_position == 1:  # Закрытие лонга
                                if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "sell", position_size, 0):
                                    current_position = 0
                                    data["account"]["position_size"] = 0.0
                                    data["account"]["stop_loss_price"] = 0.0
                                    await cancel_stop_loss(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                    position_changed = True
                                    await asyncio.sleep(20)
                                    closed_pnl = await get_bybit_closed_pnl(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                    if closed_pnl is not None:
                                        logging.info(f"Closed long at step {step} for sync, PNL: {closed_pnl}")
                                    else:
                                        warnings.append(f"Failed to get closed PNL at step {step}")
                                else:
                                    logging.error(f"Failed to close long for sync at step {step}")
                                    warnings.append(f"Failed to close long for sync at step {step}")
                            elif current_position == -1:  # Закрытие шорта
                                if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "buy", position_size, 0):
                                    current_position = 0
                                    data["account"]["position_size"] = 0.0
                                    data["account"]["stop_loss_price"] = 0.0
                                    await cancel_stop_loss(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                    position_changed = True
                                    await asyncio.sleep(20)
                                    closed_pnl = await get_bybit_closed_pnl(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                                    if closed_pnl is not None:
                                        logging.info(f"Closed short at step {step} for sync, PNL: {closed_pnl}")
                                    else:
                                        warnings.append(f"Failed to get closed PNL at step {step}")
                                else:
                                    logging.error(f"Failed to close short for sync at step {step}")
                                    warnings.append(f"Failed to close short for sync at step {step}")

                    # Открываем новую позицию, если nn_position != 0
                    if nn_position == 1:
                        current_price = await get_current_price(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                        if current_price is None:
                            logging.error(f"Failed to get price for step {step}")
                            warnings.append(f"Failed to get price for step {step}")
                        else:
                            position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                            position_size = round(max(position_size, 0.001), 3)
                            stop_loss_price = current_price * 0.9
                            logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                            if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "buy", position_size, stop_loss_price):
                                current_position = 1
                                data["account"]["position_size"] = position_size
                                data["account"]["stop_loss_price"] = stop_loss_price
                                position_changed = True
                            else:
                                logging.error(f"Failed to open long for sync at step {step}")
                                warnings.append(f"Failed to open long for sync at step {step}")
                    elif nn_position == -1:
                        current_price = await get_current_price(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                        if current_price is None:
                            logging.error(f"Failed to get price for step {step}")
                            warnings.append(f"Failed to get price for step {step}")
                        else:
                            position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                            position_size = round(max(position_size, 0.001), 3)
                            stop_loss_price = current_price * 1.1
                            logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                            if await place_bybit_order(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL, "sell", position_size, stop_loss_price):
                                current_position = -1
                                data["account"]["position_size"] = position_size
                                data["account"]["stop_loss_price"] = stop_loss_price
                                position_changed = True
                            else:
                                logging.error(f"Failed to open short for sync at step {step}")
                                warnings.append(f"Failed to open short for sync at step {step}")

                # Обновляем last_processed_step
                data["account"]["last_processed_step"] = step
                data["account"]["last_update_action"] = action
                data["account"]["last_update_position"] = nn_position

        # Баланс после действий
        final_balance = await get_bybit_balance(data["account"]["api_key"], data["account"]["api_secret"])
        if final_balance is None:
            logging.error(f"Failed to fetch final balance for account {data['account']['id']}")
            final_balance = initial_balance

        data["account"]["current_position"] = current_position
        data["account"]["balance"] = final_balance
        logging.info(f"Updated account {data['account']['id']}: position={current_position}, balance={final_balance}, last_processed_step={data['account']['last_processed_step']}")

        return True, initial_position, final_balance, position_changed, closed_pnl, warnings
    except Exception as e:
        logging.error(f"Failed to sync account {data['account']['id']}: {e}")
        return False, initial_position, initial_balance, False, None, warnings
    
async def main():
    """Основная функция."""
    data = read_accounts()
    if data is None:
        logging.error("Skipping sync due to accounts.json read error")
        return

    if data["account"]["platform"] == "bybit":
        success, initial_position, final_balance, position_changed, closed_pnl, warnings = await sync_bybit_account(data)
        if success:
            # Определяем action_str для последнего действия
            last_action = data["account"].get("last_update_action", 2)
            last_position = data["account"].get("last_update_position", 0)
            if last_action == 0 and last_position == 1:
                action_str = "Long"
            elif last_action == 1 and last_position == -1:
                action_str = "Short"
            elif (last_action == 0 or last_action == 1) and last_position == 0:
                action_str = "Close"
            else:
                action_str = "No action"

            # Получаем текущую цену
            price = ""
            if position_changed:
                current_price = await get_current_price(data["account"]["api_key"], data["account"]["api_secret"], SYMBOL)
                if current_price is not None:
                    price = str(current_price)
                else:
                    warnings.append("Failed to get current price")
            log_lines = []
            capture = False
            with open("bybit_trading.log", "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("20") and "Processing step" in line:
                        capture = True
                        log_lines = [line]
                    elif capture and line.startswith("20"):
                        log_lines.append(line)
            for line in log_lines:
                if "WARNING" in line:
                    warning_parts = line.split("WARNING: ")
                    if len(warning_parts) > 1:
                        warning = warning_parts[1].strip()
                        if warning not in warnings:
                            warnings.append(warning)

            # Отправляем лог в Telegram, если позиция изменилась
            if position_changed:
                await send_log_to_telegram(
                    action_str,
                    str(final_balance),
                    str(data["account"].get("balance", final_balance)),
                    price,
                    str(data["account"].get("position_size", "")),
                    str(data["account"].get("stop_loss_price", "")),
                    closed_pnl,
                    warnings
                )

            update_accounts(data)

if __name__ == "__main__":
    asyncio.run(main())