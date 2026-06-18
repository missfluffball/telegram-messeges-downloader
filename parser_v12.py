#импорт всех нужных библиотек
import logging
import time

import backoff
import toml
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument
from telethon.tl.types import MessageMediaPhoto
from telethon.tl.types import MessageMediaWebPage
from telethon.tl.types import MessageMediaGeo
from telethon.tl.types import MessageMediaVenue
from telethon.tl.types import MessageMediaPoll
from telethon.tl.types import MessageMediaContact
from telethon.tl.types import MessageMediaDice
from telethon.tl.types import MessageService
from telethon.errors import SessionPasswordNeededError
from telethon.errors import SessionExpiredError
from telethon import errors
import os
import asyncio
import pandas as pd
from datetime import datetime
import pytz
import re
import openpyxl

import traceback

from telethon.tl.types import InputPeerChannel

#инициальзация логгера (ведет логи)
_logger = logging.getLogger(__name__)

#функция-костыль. приводит время (datetime или timestamp) к местной временной зоне
def local_time(time):
    utc_timestamp = time
    utc_dt = utc_timestamp.astimezone(pytz.utc)
    local_tz = datetime.now().astimezone().tzinfo
    local_dt = utc_dt.astimezone(local_tz)
    return local_dt

#функция, которая конвертирует время в виде строки в формат datetime
def to_time(str_time):
    if not str_time:
        str_time = ''
    else:
        pass
    if len(str_time) == 7:
        year = int(str_time[:4])
        month = int(str_time[5:])
        if month in [1, 3, 5, 7, 8, 10, 12]:
            day = 31
        elif month in [4, 6, 9, 11]:
            day = 30
        else:
            day = 28
        hour = 23
        min_ = 59
        sec = 59
    elif len(str_time) >= 19:
        year = int(str_time[:4])
        month = int(str_time[5:7])
        day = int(str_time[8:10])
        hour = int(str_time[11:13])
        min_ = int(str_time[14:16])
        sec = int(str_time[17:19])
    else:
        year = 2010
        month = 1
        day = 1
        hour = 1
        min_ = 1
        sec = 1
    return pytz.timezone('Europe/Moscow').localize(datetime(year, month, day, hour, min_, sec))

#добовляет ко времени таймзону
def add_tz(date):
    return pytz.timezone('Europe/Moscow').localize(date)

#low and behold, класс парсера
class TelegramDownloader:
    #функция инициализации парсера. она сохраняет входные данные в переменные класса для дальнейшего использования
    def __init__(self, phone, api_id, api_hash, target_channels, target_channel_date_min, target_channel_date_max, hashes, download_dir, num_threads=3):
        target_channels_1 = [channel.lower() for channel in target_channels]

        self.client = TelegramClient(phone, api_id, api_hash, system_version = "4.16.30-vxCUSTOM", connection_retries = -1, retry_delay = 20)
        self.target_channels = target_channels_1
        self.download_dir = download_dir
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            _logger.info(f"Created directory: {self.download_dir}")
        self.phone = phone
        self.num_threads = num_threads

        self.collected_data = {key: {} for key in target_channels_1}
        self.target_date_min = dict(zip(target_channels_1, target_channel_date_min))
        self.target_date_max = dict(zip(target_channels_1, target_channel_date_max))
        
        self.date_save = {}
        self.channel_tag = {key: '' for key in target_channels_1}
        self.extended_channel_data = {'channel': [], 'id': [], 'access_hash': []}
        self.hashes_df = hashes
    
    #функция запуска парсера
    async def start(self):
        await self.client.start(self.phone)
        _logger.info("Client Created")
        #проверяем авторизацию
        if not await self.client.is_user_authorized():
            await self.client.send_code_request(self.phone)
            try:
                await self.client.sign_in(self.phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                await self.client.sign_in(password=input('Password: '))
            except Exception as e:
                _logger.info(f"Failed to authenticate: {e}")
                return
        #обращаемся к функции-обработчику каналов
        await self.process_channels()

    #функия сохранения сообщений
    async def save_mess(self):
        for channel in self.target_channels:
            flatten_data = []
            print('GOT TO MESSAGES SAVING')
            #print('data collected:')
            #print(self.collected_data)
            for message in self.collected_data[channel]:
                flatten_data.append(self.collected_data[channel][message])
            if flatten_data:
                self.channel_tag[channel] = 'parsed'
                df = pd.DataFrame(data = flatten_data)
                df1 = df.drop(labels = 'GroupedID', axis = 1)
                date = self.date_save[channel]
                file_path = os.path.join(self.download_dir, f'collected_massages_{channel}_{date}.xlsx')
                df1.to_excel(file_path)
                _logger.info(f'Data from channel {channel} saved to {file_path}')
            else:
                print(f"channel's {channel} tag: {self.channel_tag[channel]}")
                if self.channel_tag[channel] == 'no_channel_found':
                    pass
                else:
                    self.channel_tag[channel] = 'no_data_collected'
    
    #функция сохранения хэшей
    async def save_hashes(self):
        print(self.extended_channel_data)
        file_path = os.path.join(self.download_dir, f'channels_access_hashes_{self.phone}.csv')
        rows = [[self.extended_channel_data['channel'][i], self.extended_channel_data['id'][i], self.extended_channel_data['access_hash'][i]] for i in range(len(self.extended_channel_data['channel']))]
        if os.path.exists(file_path):
            data = pd.read_csv(file_path)
            df = pd.DataFrame(data = data)
            for row in rows:
                print('row: ', row)
                #print('pause')
                #p = input()
                df.loc[len(df)] = row
            df.to_csv(file_path, index = False)
        else:
            columns = list(self.extended_channel_data.keys())#['CHANNEL_NAME', 'CHANNEL_LAST', 'CHANNEL_STATUS']
            df = pd.DataFrame(data = rows, columns = columns)
            df.to_csv(file_path, index = False)
    
    #функция сохранения логов по каналам (типа как "статус канала" (жив, мертв и т.д.))
    async def save_chan_info(self):
        rows = [[name, self.date_save[name], self.channel_tag[name]] for name in self.target_channels]
        file_path = os.path.join(self.download_dir, 'channel_info.xlsx')
        
        if os.path.exists(file_path):
            data = pd.read_excel(file_path)
            df = pd.DataFrame(data = data)
            for row in rows:
                df.loc[len(df)] = row
            df.to_excel(file_path, index = False)
        else:
            columns = ['CHANNEL_NAME', 'CHANNEL_LAST', 'CHANNEL_STATUS']
            df = pd.DataFrame(data = rows, columns = columns)
            df.to_excel(file_path, index = False)
        
        await self.save_hashes()

    #обработчик групп сообщений. в телеграмме сообщения разбиваются на части если они объемные
    #под объемными как правило подразумеваются сообщения с несколькими медиа
    #(хотя предположительно большие текстовые сообщения тоже разбиваются, но я не пробовала в одно сообщение впихнуть томик "войны и мира" так что наверняка не знаю. вывод сделан на основе того как другие мессендеры обрабатывают такие вещи)
    #кароче когда вы в телеграмме видите сообщение с несколькими картинками или с несколькими файлами
    #вам кажется что это одно сообщение
    #но на самом деле на уровне структуры это несколько сообщений
    #эта функция группирует такие сообщения так, чтобы они выглядели так как вы видите их в приложении телеграмма
    async def grouped_id_handler(self, channel):
        grouped = {}
        forDel = []

        print('')
        print('')
        print(f'Data from channel "{channel}"')
        print('(note from grouped handler)')
        #print(self.collected_data)
        print('')
        print('')
        
        for message in self.collected_data[channel]:
            if self.collected_data[channel][message]['GroupedID'] != 0:
                if self.collected_data[channel][message]['GroupedID'] not in grouped:
                    grouped[self.collected_data[channel][message]['GroupedID']] = self.collected_data[channel][message]['MessageID']
                else:
                    self.collected_data[channel][grouped[self.collected_data[channel][message]['GroupedID']]]['MessageText'] += self.collected_data[channel][message]['MessageText']
                    self.collected_data[channel][grouped[self.collected_data[channel][message]['GroupedID']]]['MediaCnt'] += self.collected_data[channel][message]['MediaCnt']
                    if self.collected_data[channel][grouped[self.collected_data[channel][message]['GroupedID']]]['Media'] != self.collected_data[channel][message]['Media']:
                        self.collected_data[channel][grouped[self.collected_data[channel][message]['GroupedID']]]['Media'] += ', ' + self.collected_data[channel][message]['Media']
                    forDel.append(self.collected_data[channel][message]['MessageID'])
        for delID in forDel:
            del self.collected_data[channel][delID]
    
    #функция, которая разбирает сообщения на части (т.е. текст, айди, дата поста и т.д.) и в таком виде сохраняет их
    async def download_text(self, message, channel_name):

        print(f"Reading message {message.id} from channel {channel_name}")
                    
        try:

            local_dt = local_time(message.date)

            # Format the local datetime as a string
            PostedOn = local_dt.strftime("%Y-%m-%d %H:%M:%S")
            
            Views = message.__dict__.get('views', None)
            Forwards = message.__dict__.get('forwards', None)
            if message.replies:
                replies = message.replies.__dict__.get('replies', None)
            else:
                replies = None

            if message.media:
                if isinstance(message.media, MessageMediaPhoto):
                    Media = 'photo'
                elif isinstance(message.media, MessageMediaDocument):
                    Media = 'document'
                elif isinstance(message.media, MessageMediaWebPage):
                    Media = 'webPage'
                elif isinstance(message.media, MessageMediaContact):
                    Media = 'contact'
                elif isinstance(message.media, MessageMediaGeo):
                    Media = 'geo'
                elif isinstance(message.media, MessageMediaVenue):
                    Media = 'venue'
                elif isinstance(message.media, MessageMediaPoll):
                    Media = 'survey'
                elif isinstance(message.media, MessageMediaDice):
                    Media = 'dice'
                else:
                    Media = 'unknown'
            else:
                Media = None

            MCnt = 0
            if Media:
                MCnt = 1

            isAForwardItself = 0
            if message.fwd_from:
                isAForwardItself = 1

            GroupedID = 0
            if message.grouped_id:
                GroupedID = message.grouped_id
            
            pattern = r'[^a-zA-Zа-яА-ЯёЁ0-9\s\.,!?;:\-#&№""$*]'               
            pre_clean_message = re.sub(pattern, ' ', message.message)
            clean_message = re.sub(r'[\s+]', ' ', pre_clean_message)

            self.collected_data[channel_name][message.id] = {
                'MessageID': message.id,
                'Channel': channel_name,
                'MessageText': clean_message,
                'PostedOn': PostedOn,
                'Views': Views,
                'Forwards': Forwards,
                'Replies': replies,
                'Media': Media,
                'MediaCnt': MCnt,
                'isAForwardItself': isAForwardItself,
                'GroupedID': GroupedID
                
            }
            
        except Exception as e:
            _logger.error(f'Error collecting message data: {e}')
            traceback.print_exc()


    #функия которая разбирает канал на сообщения. по идее, эта функция, process_channels и download_text могли бы быть
    #одной большой функцией. тогда это было бы просто что-то в духе цикла в цикле в цикле. хорошо ли что функии разбиты?
    #да, вне всяких сомнений. можно было ли разбить их лучше и логичнее? я не знаю. может быть
    async def download_text_from_channel(self, channel, date_min, date_max, cname):
        tasks = set()
        
        sleeptime = 0
        async for message in self.client.iter_messages(channel, offset_date = date_max):
            sleeptime += 1
            if sleeptime > 50:
                #os.sleep(3)
                await asyncio.sleep(3)
                sleeptime = 0
            message_time = local_time(message.date)
            print(f'Message time: {message_time}')
            print(f'Target time min: {date_min}')
            print(f'Target time max: {date_max}')
            if message_time > date_max:
                print('pass')
                pass
            else:
                if message_time < date_min:
                    break
                else:
                    sleeptime += 2
                    print('not pass')
                    if not isinstance(message, MessageService):
                        file_name = f"message_{message.id}"
                        tasks.add(asyncio.create_task(self.download_text(message, cname.lower())))

                        if len(tasks) >= self.num_threads:
                            _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if tasks:
            await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            await asyncio.gather(*tasks, return_exceptions=True)
    
    #ну и собсна, функция, итерирующая по каналам.
    #из назначения функции вытекает, что парсер технически может обрабатывать по нескольку каналов друг за другом без использования
    #скрипта parser_processer.py. в свое время этот скрипт использовался для оптимизации сохранения данных. сейчас данные, собранные парсером
    #сохраняются иначе, и технически в скрипте parser_processer.py нет острой необходимости. однако данный скрипт реализует вывод логов для
    #оценки прогресса сбора информации, так что какой-то смысл запускать парсер через него сохраняется
    async def process_channels(self):
        try:
            for target in self.target_channels:
                date_min = self.target_date_min[target]
                date_max = self.target_date_max[target]
                
                #print('a thing: ', target, ' ', date, ' ', datetime.today().strftime('%Y-%m-%d %H.%M.%S'))
                self.date_save[target] = datetime.today().strftime('%Y-%m-%d %H.%M.%S')
                print(target, ' ', date_min, ' -- ', date_max)
                print("parsing started at: ", self.date_save[target])
                try:
                    if target in list(self.hashes_df['channel']):
                        hashh = list(self.hashes_df.loc[self.hashes_df['channel'] == target]['access_hash'])[0]
                        cid = int(list(self.hashes_df.loc[self.hashes_df['channel'] == target]['id'])[0])
                        entity =  InputPeerChannel(cid, hashh)
                        print(f'ACCESS NOTE: channel {target} was supposedly accessed via hash')
                    else:
                        entity = await self.client.get_entity('@' + target)
                        print(f'ACCESS NOTE: channel {target} was supposedly accessed via entity resolution')
                        if entity:
                            cid = entity.id
                            hashh = entity.access_hash
                        else:
                            cid = 0
                            hashh = 0
                    if entity:
                        self.extended_channel_data['channel'].append(target)
                        self.extended_channel_data['id'].append(cid)
                        self.extended_channel_data['access_hash'].append(hashh)
                        _logger.info(f'Found channel: {target}')
                        #enID = entity.id
                        print('channel structure:')
                        print(entity)
                        print("press press something to continue")
                        await self.download_text_from_channel(entity, date_min, date_max, target)
                        await self.grouped_id_handler(target.lower())
                        await asyncio.sleep(3)
                    
                except ValueError as e:
                    self.channel_tag[target] = 'no_channel_found'
                    print(e)
                    print("No channel with that username exists.")
                    print(f'Now the tag for channel {target} is set to {self.channel_tag[target]}')
                except errors.FloodWaitError as e:
                    self.channel_tag[target] = 'FloodWaitError'
                    print(f"We're in a timeout for ", e.seconds, 'seconds (FloodWaitError)')
                    _logger.error("We're in a timeout for ", e.seconds, 'seconds (FloodWaitError)')
                except Exception as e:
                    _logger.error(f'Processing channel got an error: {type(e).__name__} - {e}')
            
            await self.save_mess()
            await self.save_chan_info()
                
        except SessionExpiredError:
            _logger.error('Session expired. Attempting to re-authenticate.')
            await self.start()
        _logger.info('Download complete!')
        

#тело скрипта, подтягивает входные файлы и вызывает класс парсера
async def main():
    try:
        config = toml.load("config/config.toml")
    except Exception as e:
        _logger.error(f'Failed to load config.toml: {e}')
        exit(1)
    
    try:
        channels_df = pd.read_excel('config/channels.xlsx')
        now = datetime.today().strftime('%Y-%m-%d %H.%M.%S')
        then = datetime(1970, 1, 1, 0, 0, 0)

        channels_df['MAX_DATE'] = channels_df['MAX_DATE'].fillna(now)
        channels_df['MIN_DATE'] = channels_df['MIN_DATE'].fillna(then)
        
        print(channels_df['MAX_DATE'][0], ' ', type(channels_df['MAX_DATE'][0]))

        if type(channels_df['MIN_DATE'][0]) == type('str'):
            channels_df['MIN_DATE'] = channels_df['MIN_DATE'].apply(to_time)
        else:
            pass
        if type(channels_df['MAX_DATE'][0]) == type('str'):
            channels_df['MAX_DATE'] = channels_df['MAX_DATE'].apply(to_time)
        else:
            pass
        channels_list = channels_df['CHANNEL_NAME'].tolist()
        print('min type ', type(channels_df['MIN_DATE'][0]))
        print('max type ', type(channels_df['MAX_DATE'][0]))
        try:
            channels_date_min = [add_tz(date) for date in channels_df['MIN_DATE']]#channels_df['CHANNEL_LAST'].apply(to_time(), axis = 1).tolist()
        except Exception as e:
            channels_date_min = list(channels_df['MIN_DATE'])
        try:
            channels_date_max = [add_tz(date) for date in channels_df['MAX_DATE']]
        except Exception as e:
            channels_date_max = list(channels_df['MAX_DATE'])
            
        del channels_df
    except Exception as e:
        _logger.error(f'Failed to load channels.xlsx: {e}')
        exit(1)
    try:
        hashes_df = pd.read_csv('config/channels_access_hashes.csv')
    except Exception as e:
        _logger.error(f'Failed to load channels_access_hashes.xlsx: {e}')
        t = {'channel': [], 'id': [], 'access_hash': []}
        hashes_df = pd.DataFrame(data=t)

    # validate config
    required_keys = ['phone', 'api_id', 'api_hash', 'download_directory', 'num_threads']
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        _logger.error(f'Missing configuration keys: {", ".join(missing_keys)}')
        exit(1)

    download_directory = config['download_directory']
    # Ensure download directory exists
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
    downloader = TelegramDownloader(config['phone'], config['api_id'], config['api_hash'],
                                    channels_list, channels_date_min, channels_date_max, hashes_df, config['download_directory'], config['num_threads'])
    await downloader.start()
    time.sleep(10)

#служебный блок кода. подключает логгер и запускает тело кода
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    fh = logging.FileHandler('app.log')
    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # run main app
    asyncio.run(main())
