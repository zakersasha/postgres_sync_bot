from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext

from db_queries import delete_old_general_data
from keyboards import get_sync_choice_keyboard, get_start_keyboard
from sync.bali import gather_bali_main_data, save_bali_main_data, gather_bali_units_data, prepare_bali_units_data, \
    save_bali_units_data, prepare_bali_main_data
from sync.bali_i import gather_bali_i_main_data, prepare_bali_i_main_data, save_bali_i_main_data, \
    gather_bali_i_units_data, prepare_bali_i_units_data, save_bali_i_units_data
from sync.bali_s import gather_sbali_main_data, save_sbali_main_data, prepare_sbali_main_data, gather_sbali_units_data, \
    prepare_sbali_units_data, save_sbali_units_data
from sync.dubai import gather_dubai_main_data, prepare_dubai_main_data, save_dubai_main_data, get_all_records, \
    gather_dubai_units_data, prepare_dubai_units_data, save_dubai_units_data
from sync.miami import gather_miami_main_data, prepare_miami_main_data, save_miami_main_data, gather_miami_units_data, \
    prepare_miami_units_data, save_miami_units_data
from sync.oman import gather_oman_main_data, save_oman_main_data, prepare_oman_main_data
from sync.singapore import gather_units_data, prepare_units_data, save_units_data, prepare_main_data, gather_main_data, \
    save_main_data
from sync.uk import gather_uk_units_data, save_uk_units_data, save_uk_main_data, prepare_uk_main_data, \
    gather_uk_main_data, prepare_uk_units_data


async def process_back_to_start_menu(call: types.CallbackQuery):
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_back_start(call: types.CallbackQuery, state: FSMContext):
    await state.reset_state()
    await call.message.edit_text('Выберите действие:', reply_markup=await get_start_keyboard())


async def process_sync_oman(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Oman')
    delete_old_general_data(['Muscat', 'Salalah', 'Oman'])
    oman_main_data = gather_oman_main_data()
    oman_main_data_to_save = prepare_oman_main_data(oman_main_data)
    save_oman_main_data(oman_main_data_to_save)
    print('oman general table updated')
    await call.message.edit_text('Синхронизация базы Oman завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_miami(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Miami')
    delete_old_general_data('Miami')
    miami_main_data = gather_miami_main_data()
    miami_main_data_to_save = prepare_miami_main_data(miami_main_data)
    save_miami_main_data(miami_main_data_to_save)
    print('miami general table updated')

    all_general_data = get_all_records()

    miami_units_data = gather_miami_units_data()
    miami_units_data_to_save = prepare_miami_units_data(miami_units_data, all_general_data)
    save_miami_units_data(miami_units_data_to_save)
    print('miami units table updated')
    await call.message.edit_text('Синхронизация базы Miami завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_singapore(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Singapore')
    delete_old_general_data('Singapore')
    main_data = gather_main_data()
    main_data_to_save = prepare_main_data(main_data)
    save_main_data(main_data_to_save)
    print('singapore general table updated')

    all_general_data = get_all_records()

    units_data = gather_units_data()
    units_data_to_save = prepare_units_data(units_data, all_general_data)
    save_units_data(units_data_to_save)
    print('singapore units table updated')
    await call.message.edit_text('Синхронизация базы Singapore завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_dubai(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Dubai')
    delete_old_general_data('Dubai')
    dubai_main_data = gather_dubai_main_data()
    dubai_main_data_to_save = prepare_dubai_main_data(dubai_main_data)
    save_dubai_main_data(dubai_main_data_to_save)
    print('dubai general table updated')

    all_general_data = get_all_records()

    dubai_units_data = gather_dubai_units_data()
    dubai_units_data_to_save = prepare_dubai_units_data(dubai_units_data, all_general_data)
    save_dubai_units_data(dubai_units_data_to_save)
    print('dubai units table updated')
    await call.message.edit_text('Синхронизация базы Dubai завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_bali_m(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Bali MARV')
    bali_main_data = gather_bali_main_data()
    bali_main_data_to_save = prepare_bali_main_data(bali_main_data)
    save_bali_main_data(bali_main_data_to_save)
    print('bali MARV general table updated')

    all_general_data = get_all_records()

    bali_units_data = gather_bali_units_data()
    bali_units_data_to_save = prepare_bali_units_data(bali_units_data, all_general_data)
    save_bali_units_data(bali_units_data_to_save)
    print('bali MARV units table updated')
    await call.message.edit_text('Синхронизация базы Bali MARV завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_bali_i(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Bali Intermark')
    delete_old_general_data(['Bukit Peninsula', 'Gili Trawangan Island', 'Canggu', 'Ubud'])
    bali_i_main_data = gather_bali_i_main_data()
    bali_i_main_data_to_save = prepare_bali_i_main_data(bali_i_main_data)
    save_bali_i_main_data(bali_i_main_data_to_save)
    print('bali Intermark general table updated')

    all_general_data = get_all_records()

    bali_i_units_data = gather_bali_i_units_data()
    bali_i_units_data_to_save = prepare_bali_i_units_data(bali_i_units_data, all_general_data)
    save_bali_i_units_data(bali_i_units_data_to_save)
    print('bali Intermark units table updated')
    await call.message.edit_text('Синхронизация базы Bali Intermark завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_uk(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы UK')
    delete_old_general_data(['Birmingham', 'Manchester', 'Liverpool', 'Greater Manchester'])
    uk_main_data = gather_uk_main_data()
    uk_main_data_to_save = prepare_uk_main_data(uk_main_data)
    save_uk_main_data(uk_main_data_to_save)
    print('uk general table updated')

    all_general_data = get_all_records()

    uk_units_data = gather_uk_units_data()
    uk_units_data_to_save = prepare_uk_units_data(uk_units_data, all_general_data)
    save_uk_units_data(uk_units_data_to_save)
    print('uk units table updated')
    await call.message.edit_text('Синхронизация базы UK завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


async def process_sync_bali_s(call: types.CallbackQuery):
    await call.message.edit_text('Выполняется синхронизация базы Bali Saola')
    bali_s_main_data = gather_sbali_main_data()
    bali_s_main_data_to_save = prepare_sbali_main_data(bali_s_main_data)
    save_sbali_main_data(bali_s_main_data_to_save)
    print('bali saola general table updated')

    all_general_data = get_all_records()

    bali_s_units_data = gather_sbali_units_data()
    bali_s_units_data_to_save = prepare_sbali_units_data(bali_s_units_data, all_general_data)
    save_sbali_units_data(bali_s_units_data_to_save)
    print('bali saola units table updated')
    await call.message.edit_text('Синхронизация базы Bali saola завершена!')
    await call.message.answer('Выберите базу для синхронизации с postgresql:',
                              reply_markup=await get_sync_choice_keyboard())


def register_callbacks(dp: Dispatcher):
    """Register bot callbacks and triggers."""
    dp.register_callback_query_handler(process_back_to_start_menu, lambda c: c.data == 'synchronize')
    dp.register_callback_query_handler(process_back_start, lambda c: c.data == 'back_start')

    # SYNC
    dp.register_callback_query_handler(process_sync_dubai, lambda c: c.data == 'sync_dubai')
    dp.register_callback_query_handler(process_sync_miami, lambda c: c.data == 'sync_miami')
    dp.register_callback_query_handler(process_sync_singapore, lambda c: c.data == 'sync_singapore')
    dp.register_callback_query_handler(process_sync_bali_m, lambda c: c.data == 'sync_bali_m')
    dp.register_callback_query_handler(process_sync_bali_s, lambda c: c.data == 'sync_bali_s')
    dp.register_callback_query_handler(process_sync_bali_i, lambda c: c.data == 'sync_bali_i')
    dp.register_callback_query_handler(process_sync_oman, lambda c: c.data == 'sync_oman')
    dp.register_callback_query_handler(process_sync_uk, lambda c: c.data == 'sync_uk')
