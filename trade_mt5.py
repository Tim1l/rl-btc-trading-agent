import pandas as pd
import MetaTrader5 as mt5
import time
import logging
import json
import asyncio
import telegram
from datetime import datetime

# Настройки
SYMBOL = "BTCUSD"  # Символ для торговли
ACCOUNTS_FILE = "mt5_account.json"
TELEGRAM_TOKEN = ""  # Твой токен
TELEGRAM_CHANNEL = ""  # Твой ID канала
LOT_SIZE = 0.1  # Размер лота для ордеров

# Логирование
logging.basicConfig(
    filename="mt5_trading.log",
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

async def get_current_price(symbol):
    """Получает текущую цену символа через MT5."""
    try:
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            logging.error(f"Failed to get price for {symbol}: {mt5.last_error()}")
            return None
        price = tick.ask
        logging.info(f"Current price: {price}")
        return price
    except Exception as e:
        logging.error(f"Failed to get price: {e}")
        return None

async def get_mt5_position(symbol):
    """Получает текущую позицию на MT5."""
    try:
        positions = mt5.positions_get(symbol=symbol)
        if positions is None:
            logging.error(f"Failed to get position: {mt5.last_error()}")
            return None, None
        if not positions:
            return 0, None
        pos = positions[0]
        position = 1 if pos.type == mt5.ORDER_TYPE_BUY else -1
        return position, pos.ticket
    except Exception as e:
        logging.error(f"Failed to get position: {e}")
        return None, None

async def get_mt5_balance():
    """Получает текущий баланс и эквити на MT5."""
    try:
        account_info = mt5.account_info()
        if account_info is None:
            logging.error(f"Failed to get account info: {mt5.last_error()}")
            return None
        total_balance = account_info.equity  # Используем только эквити
        logging.info(f"Total equity: {total_balance}")
        return total_balance
    except Exception as e:
        logging.error(f"Failed to get balance: {e}")
        return None

async def cancel_stop_loss(symbol):
    """Отменяет все стоп-ордера для символа в MT5."""
    try:
        orders = mt5.orders_get(symbol=symbol)
        if orders is None:
            logging.error(f"Failed to get orders: {mt5.last_error()}")
            return False
        for order in orders:
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order.ticket
            }
            result = mt5.order_send(request)
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logging.error(f"Failed to cancel stop-loss order {order.ticket}: {result.comment}")
                return False
            logging.info(f"Cancelled stop-loss order {order.ticket}")
        return True
    except Exception as e:
        logging.error(f"Failed to cancel stop-loss: {e}")
        return False

async def place_mt5_order(symbol, side, amount, stop_loss_price):
    """Отправляет ордер и стоп-лосс в MT5, возвращает тикет позиции."""
    try:
        current_price = await get_current_price(symbol)
        if current_price is None:
            return False, None

        # Основной ордер
        order_type = mt5.ORDER_TYPE_BUY if side.lower() == "buy" else mt5.ORDER_TYPE_SELL
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": amount,
            "type": order_type,
            "price": current_price,
            "deviation": 20,
            "magic": 123456,
            "comment": f"RL {side.capitalize()}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            logging.error(f"Failed to place order: {result.comment}")
            return False, None
        position_ticket = result.order
        logging.info(f"Order placed: {side} {amount} {symbol}, position ticket: {position_ticket}")

        # Стоп-лосс
        if stop_loss_price > 0:
            stop_request = {
                "action": mt5.TRADE_ACTION_SLTP,
                "symbol": symbol,
                "sl": stop_loss_price,
                "position": position_ticket
            }
            stop_result = mt5.order_send(stop_request)
            if stop_result.retcode != mt5.TRADE_RETCODE_DONE:
                logging.error(f"Failed to place stop-loss: {stop_result.comment}")
                return False, None
            logging.info(f"Stop-loss placed: {stop_loss_price}")
        return True, position_ticket
    except Exception as e:
        logging.error(f"Failed to place order/stop-loss: {e}")
        return False, None

async def close_mt5_position(symbol, position_ticket, amount):
    """Закрывает существующую позицию в MT5 по тикету."""
    try:
        positions = mt5.positions_get(symbol=symbol)
        if positions is None or not positions:
            logging.error(f"No positions found to close: {mt5.last_error()}")
            return False

        position = None
        for pos in positions:
            if pos.ticket == position_ticket:
                position = pos
                break

        if position is None:
            logging.error(f"Position with ticket {position_ticket} not found")
            return False

        current_price = await get_current_price(symbol)
        if current_price is None:
            return False

        # Определяем тип ордера для закрытия
        close_type = mt5.ORDER_TYPE_SELL if position.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": amount,
            "type": close_type,
            "position": position.ticket,
            "price": current_price,
            "deviation": 20,
            "magic": 123456,
            "comment": "RL Close Position",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            logging.error(f"Failed to close position: {result.comment}")
            return False
        logging.info(f"Position {position.ticket} closed: {amount} {symbol}")
        return True
    except Exception as e:
        logging.error(f"Failed to close position: {e}")
        return False

async def get_mt5_closed_pnl(position_ticket):
    """Получает PNL закрытой позиции по её тикету."""
    try:
        # Добавляем задержку для обновления истории
        await asyncio.sleep(2)
        
        # Получаем сделки, связанные с позицией
        logging.info(f"Fetching deals for position ticket {position_ticket}")
        deals = mt5.history_deals_get(position=position_ticket)
        if deals is None:
            logging.error(f"Failed to get deals for position {position_ticket}: {mt5.last_error()}")
            return None
        if not deals:
            logging.error(f"No deals found for position {position_ticket}")
            return None

        # Логируем все сделки для отладки
        logging.info(f"Total deals found for position {position_ticket}: {len(deals)}")
        for deal in deals:
            deal_info = {
                "ticket": deal.ticket,
                "time": datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d %H:%M:%S'),
                "symbol": deal.symbol,
                "type": deal.type,
                "entry": deal.entry,
                "profit": deal.profit,
                "volume": deal.volume,
                "position_id": deal.position_id
            }
            logging.info(f"Deal: {deal_info}")

        # Ищем сделку с типом DEAL_ENTRY_OUT (закрытие позиции)
        for deal in deals:
            if deal.entry == mt5.DEAL_ENTRY_OUT:
                closed_pnl = deal.profit
                logging.info(f"Closed deal for position {position_ticket}: ticket={deal.ticket}, time={datetime.fromtimestamp(deal.time)}, profit={closed_pnl}")
                return closed_pnl
        logging.error(f"No closed deals found for position {position_ticket}")
        return None
    except Exception as e:
        logging.error(f"Failed to get closed PNL for position {position_ticket}: {e}")
        return None

def read_accounts():
    """Читает mt5_account.json."""
    try:
        with open(ACCOUNTS_FILE, "r") as f:
            data = json.load(f)
            return data  # Возвращаем весь объект, включая last_update и account
    except Exception as e:
        logging.error(f"Failed to read {ACCOUNTS_FILE}: {e}")
        return None

def update_accounts(data):
    """Обновляет mt5_account.json."""
    try:
        with open(ACCOUNTS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Updated {ACCOUNTS_FILE}")
    except Exception as e:
        logging.error(f"Failed to update {ACCOUNTS_FILE}: {e}")

async def send_log_to_telegram(action, balance, initial_balance, price, position_size, stop_loss, closed_pnl, warnings):
    """Отправляет краткий лог в Telegram-канал в человеческом формате."""
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        formatted_log = "📊 MT5 Trading Update 📊\n\n"
        formatted_log += f"💰 Balance: {balance} USDT\n"
        formatted_log += f"📈 BTCUSD price: {price}\n" if price else ""
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
                text=f"MT5 Account 1:\n{part}",
                parse_mode="HTML"
            )
        logging.info(f"Log sent to Telegram channel in {len(parts)} parts")
    except Exception as e:
        logging.error(f"Failed to send log to Telegram: {e}")

async def sync_mt5_account(data):
    """Получает позицию и баланс с MT5, обрабатывает все необработанные действия начиная с шага 961."""
    try:
        # Подключаемся к MT5
        if not mt5.initialize():
            logging.error("Failed to initialize MT5")
            return False, None, None, False, None, []
        if not mt5.login(int(data["account"]["account_id"]), data["account"]["password"], data["account"]["server"]):
            logging.error(f"Failed to login to MT5 for account {data['account']['id']}: {mt5.last_error()}")
            mt5.shutdown()
            return False, None, None, False, None, []

        # Получаем последний обработанный шаг, по умолчанию 0 для нового запуска
        last_processed_step = data["account"].get("last_processed_step", 0)
        start_step = 961  # Реальная торговля начинается с шага 961
        pending_actions = read_last_action(last_processed_step, start_step=start_step)

        # Баланс и позиция до действий
        initial_position, initial_position_ticket = await get_mt5_position(SYMBOL)
        initial_balance = await get_mt5_balance()
        if initial_position is None or initial_balance is None:
            logging.error(f"Failed to fetch position or balance for account {data['account']['id']}")
            mt5.shutdown()
            return False, initial_position, initial_balance, False, None, []

        current_position = initial_position
        current_position_ticket = initial_position_ticket
        position_changed = False
        closed_pnl = None
        warnings = []

        # # Проверяем синхронизацию позиции
        # nn_position = data["account"].get("last_update_position", 0)
        # if nn_position != initial_position:
        #     warning = f"Position mismatch: NN={nn_position}, MT5={initial_position}"
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
                    current_price = await get_current_price(SYMBOL)
                    if current_price is None:
                        logging.error(f"Failed to get price for step {step}")
                        warnings.append(f"Failed to get price for step {step}")
                    else:
                        position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                        position_size = max(position_size, LOT_SIZE)
                        stop_loss_price = current_price * 0.9
                        logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                        success, new_position_ticket = await place_mt5_order(SYMBOL, "buy", position_size, stop_loss_price)
                        if success:
                            current_position = 1
                            current_position_ticket = new_position_ticket
                            data["account"]["position_size"] = position_size
                            data["account"]["stop_loss_price"] = stop_loss_price
                            data["account"]["position_ticket"] = new_position_ticket
                            position_changed = True
                        else:
                            logging.error(f"Failed to open long at step {step}")
                            warnings.append(f"Failed to open long at step {step}")

                # Открытие шорта
                elif action == 1 and current_position == 0 and nn_position == -1:
                    current_price = await get_current_price(SYMBOL)
                    if current_price is None:
                        logging.error(f"Failed to get price for step {step}")
                        warnings.append(f"Failed to get price for step {step}")
                    else:
                        position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                        position_size = max(position_size, LOT_SIZE)
                        stop_loss_price = current_price * 1.1
                        logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                        success, new_position_ticket = await place_mt5_order(SYMBOL, "sell", position_size, stop_loss_price)
                        if success:
                            current_position = -1
                            current_position_ticket = new_position_ticket
                            data["account"]["position_size"] = position_size
                            data["account"]["stop_loss_price"] = stop_loss_price
                            data["account"]["position_ticket"] = new_position_ticket
                            position_changed = True
                        else:
                            logging.error(f"Failed to open short at step {step}")
                            warnings.append(f"Failed to open short at step {step}")

                # Закрытие позиции
                elif (action == 0 or action == 1) and nn_position == 0 and current_position != 0:
                    position_size = data["account"].get("position_size", 0.0)
                    position_ticket = data["account"].get("position_ticket", current_position_ticket)
                    if position_size > 0 and position_ticket is not None:
                        if current_position == 1:  # Закрытие лонга
                            if await close_mt5_position(SYMBOL, position_ticket, position_size):
                                closed_pnl = await get_mt5_closed_pnl(position_ticket)
                                current_position = 0
                                current_position_ticket = None
                                data["account"]["position_size"] = 0.0
                                data["account"]["stop_loss_price"] = 0.0
                                data["account"]["position_ticket"] = None
                                data["account"]["last_closed_pnl"] = closed_pnl
                                await cancel_stop_loss(SYMBOL)
                                position_changed = True
                                if closed_pnl is not None:
                                    logging.info(f"Closed long at step {step}, PNL: {closed_pnl}")
                                else:
                                    warnings.append(f"Failed to get closed PNL at step {step}")
                            else:
                                logging.error(f"Failed to close long at step {step}")
                                warnings.append(f"Failed to close long at step {step}")
                        elif current_position == -1:  # Закрытие шорта
                            if await close_mt5_position(SYMBOL, position_ticket, position_size):
                                closed_pnl = await get_mt5_closed_pnl(position_ticket)
                                current_position = 0
                                current_position_ticket = None
                                data["account"]["position_size"] = 0.0
                                data["account"]["stop_loss_price"] = 0.0
                                data["account"]["position_ticket"] = None
                                data["account"]["last_closed_pnl"] = closed_pnl
                                await cancel_stop_loss(SYMBOL)
                                position_changed = True
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
                        position_ticket = data["account"].get("position_ticket", current_position_ticket)
                        if position_size > 0 and position_ticket is not None:
                            if await close_mt5_position(SYMBOL, position_ticket, position_size):
                                closed_pnl = await get_mt5_closed_pnl(position_ticket)
                                current_position = 0
                                current_position_ticket = None
                                data["account"]["position_size"] = 0.0
                                data["account"]["stop_loss_price"] = 0.0
                                data["account"]["position_ticket"] = None
                                data["account"]["last_closed_pnl"] = closed_pnl
                                await cancel_stop_loss(SYMBOL)
                                position_changed = True
                                if closed_pnl is not None:
                                    logging.info(f"Closed position at step {step} for sync, PNL: {closed_pnl}")
                                else:
                                    warnings.append(f"Failed to get closed PNL at step {step}")
                            else:
                                logging.error(f"Failed to close position for sync at step {step}")
                                warnings.append(f"Failed to close position for sync at step {step}")

                    # Открываем новую позицию, если nn_position != 0
                    if nn_position == 1:
                        current_price = await get_current_price(SYMBOL)
                        if current_price is None:
                            logging.error(f"Failed to get price for step {step}")
                            warnings.append(f"Failed to get price for step {step}")
                        else:
                            position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                            position_size = max(position_size, LOT_SIZE)
                            stop_loss_price = current_price * 0.9
                            logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                            success, new_position_ticket = await place_mt5_order(SYMBOL, "buy", position_size, stop_loss_price)
                            if success:
                                current_position = 1
                                current_position_ticket = new_position_ticket
                                data["account"]["position_size"] = position_size
                                data["account"]["stop_loss_price"] = stop_loss_price
                                data["account"]["position_ticket"] = new_position_ticket
                                position_changed = True
                            else:
                                logging.error(f"Failed to open long for sync at step {step}")
                                warnings.append(f"Failed to open long for sync at step {step}")
                    elif nn_position == -1:
                        current_price = await get_current_price(SYMBOL)
                        if current_price is None:
                            logging.error(f"Failed to get price for step {step}")
                            warnings.append(f"Failed to get price for step {step}")
                        else:
                            position_size = data["account"]["deposit"] * data["account"]["risk_coeff"] / current_price
                            position_size = max(position_size, LOT_SIZE)
                            stop_loss_price = current_price * 1.1
                            logging.info(f"Calculated position size: {position_size}, stop_loss_price: {stop_loss_price}")
                            success, new_position_ticket = await place_mt5_order(SYMBOL, "sell", position_size, stop_loss_price)
                            if success:
                                current_position = -1
                                current_position_ticket = new_position_ticket
                                data["account"]["position_size"] = position_size
                                data["account"]["stop_loss_price"] = stop_loss_price
                                data["account"]["position_ticket"] = new_position_ticket
                                position_changed = True
                            else:
                                logging.error(f"Failed to open short for sync at step {step}")
                                warnings.append(f"Failed to open short for sync at step {step}")

                # Обновляем last_processed_step
                data["account"]["last_processed_step"] = step
                data["account"]["last_update_action"] = action
                data["account"]["last_update_position"] = nn_position

        # Баланс после действий
        final_balance = await get_mt5_balance()
        if final_balance is None:
            logging.error(f"Failed to fetch final balance for account {data['account']['id']}")
            final_balance = initial_balance

        data["account"]["current_position"] = current_position
        data["account"]["balance"] = final_balance
        logging.info(f"Updated account {data['account']['id']}: position={current_position}, balance={final_balance}, last_processed_step={data['account']['last_processed_step']}")

        mt5.shutdown()
        return True, initial_position, final_balance, position_changed, closed_pnl, warnings
    except Exception as e:
        logging.error(f"Failed to sync account {data['account']['id']}: {e}")
        mt5.shutdown()
        return False, initial_position, initial_balance, False, None, warnings
    
async def main():
    """Основная функция."""
    data = read_accounts()
    if data is None:
        logging.error("Skipping sync due to accounts.json read error")
        return

    if data["account"]["platform"] == "mt5":
        success, initial_position, final_balance, position_changed, closed_pnl, warnings = await sync_mt5_account(data)
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

            # Получаем текущую цену из логов
            price = ""
            log_lines = []
            capture = False
            with open("mt5_trading.log", "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("20") and "Processing step" in line:
                        capture = True
                        log_lines = [line]
                    elif capture and line.startswith("20"):
                        log_lines.append(line)
            for line in log_lines:
                if "Current price" in line:
                    price = line.split("Current price: ")[1].strip()
                elif "WARNING" in line:
                    warning_parts = line.split("WARNING: ")
                    if len(warning_parts) > 1:
                        warning = warning_parts[1].strip()
                        if warning not in warnings and warning not in warnings:
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