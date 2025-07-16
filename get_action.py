import os
import warnings
import torch
import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime
import random
from mvp_architecture import MaskedActorCriticPolicy, DictTradingEnv, policy_kwargs
from stable_baselines3 import PPO

# --- Create folder for TensorBoard logs ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Create folder if it doesn't exist

# --- Environment options ---
os.environ["TORCHINDUCTOR_DISABLE"] = "1"
os.environ["TORCHDYNAMO_DISABLE"] = "1"
os.environ["CUDA_VISIBLE_DEVICES"] = ""
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings("ignore", category=UserWarning)
torch._dynamo.config.suppress_errors = True
logging.getLogger().setLevel(logging.ERROR)
np.NaN = np.nan

# === Parameters ===
import argparse
parser = argparse.ArgumentParser(description="Run trading action prediction")
parser.add_argument("--data-file", type=str, default="BTCUSDT_calc.csv", help="Path to the data CSV file")
args = parser.parse_args()
DATA_FILE = args.data_file
# DATA_FILE = "BTCUSDT_calc.csv"
MODEL_FILE = "best_rl_ever.zip"
LOOKBACK = 480
HISTORY_FILE = "rl_actions_history.csv"

# --- 1. Load history if it exists ---
if os.path.exists(HISTORY_FILE):
    prev_results = pd.read_csv(HISTORY_FILE)
    last_logged_step = int(prev_results["step"].iloc[-1])
    print(f"[INFO] Continuing from step {last_logged_step + 1}")
else:
    prev_results = None
    last_logged_step = LOOKBACK - 1

# --- 2. Load data ---
df = pd.read_csv(DATA_FILE, parse_dates=['DATETIME'])
# Set DATETIME as index
df.set_index('DATETIME', inplace=True)
# Check for env_state.json to retrieve initial_run_date
initial_run_date = None
if os.path.exists("env_state.json"):
    try:
        with open("env_state.json", "r") as f:
            env_state = json.load(f)
        initial_run_date = env_state.get("initial_run_date")
        if initial_run_date:
            initial_run_date = pd.to_datetime(initial_run_date)
    except Exception as e:
        print(f"[WARNING] Error loading initial_run_date from env_state.json: {e}")

# Define the starting point of the data
if initial_run_date is not None and initial_run_date in df.index:
    start_idx = df.index.get_loc(initial_run_date)
    df = df.iloc[start_idx:]
else:
    # If initial_run_date is not found or absent, take the last LOOKBACK + 480 rows
    df = df.tail(LOOKBACK + 480)
    initial_run_date = df.index[0]
    # Save initial_run_date to env_state.json
    env_state = env_state if 'env_state' in locals() else {}
    env_state["initial_run_date"] = str(initial_run_date)

# print(f"[DEBUG] Data length df: {len(df)}, last date: {df.index[-1]}, first date: {df.index[0]}")

# --- 3. Create environment ---
# print(f"[DEBUG] Before creating DictTradingEnv: df.index[0]={df.index[0]}, df.index.name={df.index.name}, type(df)={type(df)}, df.shape={df.shape}")
env = DictTradingEnv(df, lookback_window=LOOKBACK, initial_balance=10_000, verbose=0)
# print(f"[DEBUG] After creating DictTradingEnv: env.data.index[0]={env.data.index[0]}, env.data.index.name={env.data.index.name}, type(env.data)={type(env.data)}, env.data.shape={env.data.shape}")
# print(f"[DEBUG] env.action_space: {env.action_space}")
# print(f"[DEBUG] env.observation_space: {env.observation_space}")
# --- 4. Load model ---
model = PPO.load(
    MODEL_FILE,
    env=env,
    device='cpu',
    tensorboard_log=None,
    custom_objects={
        "policy_class": MaskedActorCriticPolicy,
        "policy_kwargs": policy_kwargs
    }
)
# print(f"[INFO] Model successfully loaded from {MODEL_FILE}")
# print(f"[DEBUG] model.action_space: {model.action_space}")
# print(f"[DEBUG] model.observation_space: {model.observation_space}")

def save_state_and_observation(env, obs, file_path="pre_predict_state.json"):
    state = env.get_env_state()
    with open(file_path, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    print(f"[INFO] State and observation saved to {file_path}")

# --- Restore environment state ---
env_state = None
if os.path.exists("env_state.json"):
    # print(f"[DEBUG] Found env_state.json, attempting to load")
    try:
        with open("env_state.json", "r") as f:
            env_state = json.load(f)
        # print(f"[DEBUG] Loaded env_state: current_step={env_state.get('current_step')}, position={env_state.get('position')}, len(trade_log)={len(env_state.get('trade_log', []))}")
        env.set_env_state(env_state)
        env.tech_reward_shaper.reset(initial_balance=env.net_worth)
        obs = env.get_current_observation()
        # print(f"[DEBUG] After set_env_state: current_step={env.current_step}, position={env.position}, net_worth={env.net_worth}, len(trade_log)={len(env.trade_log)}, observation_shape={obs['observation'].shape}")
        # Check last_obs shape
        # expected_shape = (len(env.data_columns) + len(env.computed_columns), env.lookback_window)
        # if env.last_obs.shape != expected_shape:
        #     print(f"[WARNING] last_obs shape mismatch: received {env.last_obs.shape}, expected {expected_shape}")
        # Save state immediately after restoration
        save_state_and_observation(env, obs, "pre_predict_state.json")
    except Exception as e:
        print(f"[ERROR] Error loading env_state.json: {e}")
        env_state = None

if env_state is None:
    print(f"[INFO] Resetting environment, env_state={'exists' if env_state else 'missing'}")
    obs, _ = env.reset()
    last_logged_step = LOOKBACK - 1
    print(f"[INFO] Environment reset, starting from step {last_logged_step + 1}")
    # Save state after reset
    save_state_and_observation(env, obs, "pre_predict_state.json")
else:
    # Set last_logged_step based on history or env_state
    if prev_results is not None:
        last_logged_step = int(prev_results["step"].iloc[-1])  # Remove +1 to sync with the last processed candle
    else:
        last_logged_step = env_state.get("current_step", LOOKBACK - 1)
    # print(f"[DEBUG] Set last_logged_step={last_logged_step}, env.current_step={env.current_step}")

# print(f"[DEBUG] Restored state: current_step={env.current_step}, position={env.position}, net_worth={env.net_worth}, action_mask={obs['action_mask'].tolist()}")

# --- 6. Main loop ---
actions_list = []
results = []

# print(f"[DEBUG] Starting from step {last_logged_step}, env.current_step={env.current_step}, len(df)={len(df)}, last_date={df.index[-1]}")

# Determine new candles based on steps
num_new_candles = max(0, len(df) - last_logged_step)
if num_new_candles <= 0:
    print(f"[WARNING] Invalid number of new candles: {num_new_candles}, last_logged_step={last_logged_step}, len(df)={len(df)}")
    num_new_candles = 0
new_candles = df.index[last_logged_step:] if num_new_candles > 0 else []
print(f"[DEBUG] New candles: {num_new_candles}, dates: {new_candles[-5:].tolist() if num_new_candles > 0 else 'none'}")

# Check for skipped candles (optional, if verbose >= 1)
if num_new_candles > 1 and env.verbose >= 1:
    last_processed_date = pd.Timestamp(env.data_dates[last_logged_step])
    first_new_candle = new_candles[0]
    time_diff = (first_new_candle - last_processed_date).total_seconds() / 60
    if time_diff > 15:
        print(f"[WARNING] Skipped {time_diff / 15:.0f} candles between {last_processed_date} and {first_new_candle}")

# --- 6. Main loop (continued) ---
for i in range(num_new_candles):
    step = last_logged_step + 1 + i
    # print(f"[DEBUG] Processing step {step}, env.current_step={env.current_step}")
    print(f"[DEBUG] Current position: {env.position}, holding steps: {env.current_step - env.last_trade_step if env.last_trade_step is not None else 0}")
    # print(f"[DEBUG] After get_current_observation, OPEN_norm last 6: {obs['observation'][0][-6:].tolist()}")
    # print(f"[DEBUG] After get_current_observation: observation_shape={obs['observation'].shape}, action_mask={obs['action_mask'].tolist()}")
    obs_tensor = {
        "observation": torch.from_numpy(obs["observation"]).float().to(model.device),
        "action_mask": torch.from_numpy(obs["action_mask"]).float().to(model.device)
    }
    # Warm up RNG
    with torch.no_grad():
        dummy_dist = torch.distributions.Categorical(probs=torch.tensor([0.33, 0.33, 0.34]))
        dummy_action = dummy_dist.sample()
        # print(f"[DEBUG] Warming up RNG: dummy_action={dummy_action.item()}")
    # Perform prediction
    with torch.no_grad():
        action, _ = model.predict(obs, deterministic=False)
        dist = model.policy.get_distribution(obs_tensor)
        action_probs = dist.distribution.probs.cpu().numpy()[0]
    print(f"[DEBUG] Action probabilities: {action_probs.tolist()}, chosen action: {action}")
    # print(f"[DEBUG] obs['observation'] last row (last 5): {obs['observation'][-1, -5:].tolist()}")
    # print(f"[DEBUG] obs['action_mask']: {obs['action_mask'].tolist()}")
    # print(f"[DEBUG] Before env.step: step={step}, env.current_step={env.current_step}, date={pd.Timestamp(env.data_dates[env.current_step])}, action={action}")
    date = pd.Timestamp(env.data_dates[env.current_step]).strftime('%Y-%m-%d %H:%M:%S')
    current_price = env.raw_close[env.current_step]
    # print(f"[DEBUG] Before step: step={step}, date={date}, current_price={current_price}")
    obs, reward, terminated, truncated, info = env.step(action)
    # print(f"[DEBUG] After env.step, OPEN_norm last 6: {obs['observation'][0][-6:].tolist()}")
    # print(f"[DEBUG] After env.step: terminated={terminated}, truncated={truncated}, env.current_step={env.current_step}, reward={reward}")
    # print(f"[DEBUG] After step: reward={reward}, net_worth={env.net_worth}, profit_history[-1]={env.profit_history[-1]}")
    done = terminated or truncated
    position = env.position
    trade_log = env.trade_log
    position_entry_price = None
    position_size = None
    trade_pnl = None
    if trade_log:
        last_trade = trade_log[-1]
        position_entry_price = last_trade.get("entry_price")
        position_size = last_trade.get("position_value")
        if position != 0:
            profit_idx = len(env.data_columns) + env.computed_columns.index('profit_norm')
            profit_row = obs["observation"][profit_idx]
            trade_pnl = profit_row[-1] * env.initial_balance * 0.01
        elif action in [0, 1]:
            trade_pnl = last_trade.get("profit", 0)
    results.append({
        "step": step,
        "date": date,
        "action": int(action),
        "reward": float(reward),
        "net_worth": env.net_worth,
        "drawdown": env.max_drawdown,
        "position": position,
        "position_entry_price": position_entry_price,
        "position_size": position_size,
        "trade_pnl": trade_pnl,
        "current_price": current_price
    })
    actions_list.append(action)
    print(f"Step {step} | Date {date} | Action: {action} | NetWorth: {env.net_worth} | Pos: {env.position}")
    if done:
        print(f"[INFO] Episode completed at step {step}, reason: {'terminated' if terminated else 'truncated'}")
        break

try:
    with open("env_state.json", "w") as f:
        json.dump(env_state, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Saved initial_run_date={initial_run_date} to env_state.json")
except Exception as e:
    print(f"[ERROR] Error saving initial_run_date to env_state.json: {e}")
# Save results
if results:
    dtypes = {
        "step": int,
        "date": str,
        "action": int,
        "reward": float,
        "net_worth": float,
        "drawdown": float,
        "position": int,
        "position_entry_price": float,
        "position_size": float,
        "trade_pnl": float,
        "current_price": float
    }
    df_results = pd.DataFrame(results).astype({k: v for k, v in dtypes.items() if k in results[0]})
    if prev_results is not None:
        df_results = pd.concat([prev_results, df_results], ignore_index=True)
    df_results.to_csv(HISTORY_FILE, index=False)
    print("Action history saved to rl_actions_history.csv")
else:
    print("[WARNING] No new results to save, skipping writing to rl_actions_history.csv")

# Save final state
env_state = env.get_env_state()
# Set initial_run_date from the variable defined in the data loading section
env_state["initial_run_date"] = str(pd.Timestamp(initial_run_date))
# print(f"[DEBUG] Set initial_run_date={env_state['initial_run_date']} in env_state")
# Adjust current_step for the last candle
if env.current_step == len(env.data) - 1:
    env_state["current_step"] = env.current_step + 1
#     print(f"[DEBUG] Adjusted env_state['current_step']={env_state['current_step']} for the last candle")
# print(f"[DEBUG] Saving state: current_step={env_state['current_step']}, current_datetime={env_state['current_datetime']}")
try:
    with open("env_state.json", "w") as f:
        json.dump(env_state, f, indent=2, ensure_ascii=False)
    print("Environment state saved to env_state.json")
except TypeError as e:
    print(f"[ERROR] Serialization error for env_state: {e}")
    print(f"[DEBUG] Problematic env_state keys: {list(env_state.keys())}")
    for key, value in env_state.items():
        if isinstance(value, (list, dict)):
            print(f"[DEBUG] Key {key}: first 5 elements or keys: {str(value)[:100]}...")
    raise
last_step = (env.current_step + 1) if num_new_candles > 0 else last_logged_step
print(f"Action at step {last_step}: {action if 'action' in locals() else None}")