from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


async def get_sync_choice_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    btn_dubai = InlineKeyboardButton('Dubai', callback_data='sync_dubai')
    btn_miami = InlineKeyboardButton('Miami', callback_data='sync_miami')
    btn_singapore = InlineKeyboardButton('Singapore', callback_data='sync_singapore')
    btn_oman = InlineKeyboardButton('Oman', callback_data='sync_oman')
    btn_bali_m = InlineKeyboardButton('Bali MARV', callback_data='sync_bali_m')
    btn_bali_s = InlineKeyboardButton('Bali Saola', callback_data='sync_bali_s')
    btn_bali_i = InlineKeyboardButton('Bali Intermark', callback_data='sync_bali_i')
    btn_uk = InlineKeyboardButton('UK', callback_data='sync_uk')
    btn_back = InlineKeyboardButton('⬅️ Назад', callback_data='back_start')

    keyboard.add(btn_dubai, btn_miami, btn_singapore, btn_oman, btn_bali_m, btn_bali_i, btn_bali_s, btn_uk, btn_back)

    return keyboard


async def get_start_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    btn_sync = InlineKeyboardButton('Синхронизировать', callback_data='synchronize')

    keyboard.add(btn_sync)
    return keyboard
