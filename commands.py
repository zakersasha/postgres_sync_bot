"""Bot supported commands."""
from aiogram import Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.types import Message

from keyboards import get_start_keyboard, get_sync_choice_keyboard


async def send_welcome(message: Message, state: FSMContext):
    await state.reset_state()
    await message.answer('Выберите действие:', reply_markup=await get_start_keyboard())


async def send_sync(message: Message):
    await message.answer('Выберите действие:', reply_markup=await get_sync_choice_keyboard())


def register_commands(dp: Dispatcher):
    """Register bot commands."""
    dp.register_message_handler(send_welcome, commands=['start'], state='*')
    dp.register_message_handler(send_sync, commands=['sync'], state='*')
