# -*- coding: utf-8 -*-
import os
import shutil
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
from middle.utils import Constants, setup_logger, extract_zip, create_directory, SemanaOperativa, get_decks_ccee
from middle.s3 import handle_webhook_file, get_latest_webhook_product


class DessemOnsToCcee:
    def __init__(self):
        self.consts = Constants()
        self.logger = setup_logger()
        self.today = datetime.now()
        self.logger.info(f"DeckProcessor initialized for date: {self.today.strftime('%Y-%m-%d')}")

    def run_process(self) -> None:
        self.logger.info("="*60)
        self.logger.info("STARTING DESSEM DECK PROCESSING ONS-TO-CCEE")
        self.logger.info("="*60)

        path_ccee = path_ons = None
        try:
            # 1. Get decks
            path_ccee = self.get_latest_deck_ccee(self.today)[0]
            path_ons  = self.get_latest_deck_ons(self.today+timedelta(days=1))

            # 2. Create base
            path_deck = self.create_deck_base(path_ons, path_ccee)

            # 3. Read data
            df_carga = self.read_load_pdo(path_ons)
            map_ccee = self.map_entdados_ccee(self.find_file(path_ccee, 'entdados.dat'))

            # 4. Process entdados
            entdados = self.find_file(path_deck, 'entdados.dat')
            entdados = self.adjust_tm(entdados)
            entdados = self.update_load(entdados, df_carga)
            entdados = self.adjust_di(entdados)
            entdados = self.uncoment_entdados(entdados, map_ccee)
            entdados = self.coment_entdados(entdados, map_ccee)
            entdados = self.adjust_di(entdados)
            entdados = self.adjust_barras(entdados, map_ccee['BARRA'])
            self.write_file(path_deck, 'entdados.dat', entdados)

            # 5. Process dessem.arq
            dessem_arq = self.find_file(path_deck, 'dessem.arq')
            dessem_arq = self.comment_arq(dessem_arq)
            self.write_file(path_deck, 'dessem.arq', dessem_arq)

            self.logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
            self.logger.info(f"Final deck generated at: {path_deck}")

        except Exception as e:
            self.logger.error(f"CRITICAL ERROR DURING EXECUTION: {e}", exc_info=True)
            raise
        finally:
            if path_ccee and path_ons:
                self.cleanup(path_ccee, path_ons)
            self.logger.info("EXECUTION FINISHED")
            self.logger.info(f"Returning path deck: {path_deck}")
            return path_deck
    
    def get_latest_deck_ccee(self, data: datetime) -> str:
        self.logger.info(f"Fetching latest CCEE deck for base date: {data.strftime('%Y-%m-%d')}")
        path_deck = get_decks_ccee(
            path=self.consts.PATH_ARQUIVOS_TEMP,
            deck='dessem',
            file_name=self.consts.CCEE_DECK_DESSEM
        )
        self.logger.info(f"CCEE deck downloaded: {path_deck}")

        path_deck_unzip = extract_zip(path_deck)
        self.logger.info(f"CCEE deck extracted to: {path_deck_unzip}")

        files = os.listdir(path_deck_unzip)
        decks = [f for f in files if 'Resultado' not in f]
        self.logger.debug(f"Available decks (excluding Resultado): {decks}")

        for days in range(5):
            date_check = data - timedelta(days=days)
            rv = int(SemanaOperativa(date_check).current_revision)
            pattern = f'RV{rv}D{str(date_check.day).zfill(2)}'
            deck = [f for f in decks if pattern in f]

            if deck:
                latest_deck = max(deck, key=lambda x: os.path.getctime(os.path.join(path_deck_unzip, x)))
                full_path = os.path.join(path_deck_unzip, latest_deck)
                full_path_result = os.path.join(path_deck_unzip, 'Resultado_'+latest_deck)
                self.logger.info(f"CCEE deck selected: {latest_deck} (RV{rv}, day {date_check.day})")
                return [extract_zip(full_path), extract_zip(full_path_result)]

        error_msg = "No CCEE deck found in the last 5 days."
        self.logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    def get_latest_deck_ons(self, data: datetime) -> str:
        self.logger.info(f"Fetching latest ONS deck via webhook for: {data.strftime('%Y-%m-%d')}")
        df_payload = pd.DataFrame(get_latest_webhook_product(self.consts.WEBHOOK_DECK_DESSEM))
        data_str = data.strftime("%Y-%m-%d")
        payload = df_payload[df_payload['periodicidade'].str.contains(data_str)]

        if payload.empty:
            error_msg = f"No ONS deck found for date {data_str}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"ONS payload found: {dict(payload.iloc[0])}")
        path_deck = handle_webhook_file(dict(payload.iloc[0]), self.consts.PATH_ARQUIVOS_TEMP)
        self.logger.info(f"ONS deck downloaded: {path_deck}")

        path_deck_unzip = extract_zip(path_deck)
        self.logger.info(f"ONS deck extracted to: {path_deck_unzip}")
        return path_deck_unzip

    def create_deck_base(self, path_ons: str, path_ccee: str) -> str:
        self.logger.info(f"Creating combined ONS + CCEE base deck")
        self.logger.debug(f"ONS path: {path_ons}")
        self.logger.debug(f"CCEE path: {path_ccee}")

        rv = path_ccee.split('RV')[1][:1]
        files_ccee_to_copy = [
            f'cortdeco.rv{rv}', 'rmpflx.dat', f'mapcut.rv{rv}', 'restseg.dat', 'rstlpp.dat'
        ]
        self.logger.debug(f"CCEE files to copy: {files_ccee_to_copy}")

        path_dest = path_ons.replace('ONS', 'ONS-TO-CCEE')
        create_directory(path_dest, '')
        self.logger.info(f"Base directory created: {path_dest}")

        # Copy ONS files
        copied_ons = 0
        for file in os.listdir(path_ons):
            if not file.endswith('.afp') and not file.endswith('.pwf') and not file.startswith('pdo_'):
                src = os.path.join(path_ons, file)
                dst = os.path.join(path_dest, file)
                shutil.copyfile(src, dst)
                copied_ons += 1
        self.logger.info(f"{copied_ons} ONS files copied (excluding .afp, .pwf, pdo_*)")

        # Copy CCEE files
        copied_ccee = 0
        for file in os.listdir(path_ccee):
            if file in files_ccee_to_copy:
                src = os.path.join(path_ccee, file)
                dst = os.path.join(path_dest, file)
                shutil.copyfile(src, dst)
                copied_ccee += 1
                self.logger.debug(f"CCEE file copied: {file}")
        self.logger.info(f"{copied_ccee} specific CCEE files copied")

        return path_dest

    def find_file(self, directory: str, file_find: str) -> List[str]:
        self.logger.info(f"Searching for file with prefix '{file_find}' in: {directory}")
        for file in os.listdir(directory):
            if file.lower().startswith(file_find.lower()):
                file_path = os.path.join(directory, file)
                self.logger.info(f"File found: {file}")
                with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                    lines = f.readlines()
                self.logger.debug(f"File read: {len(lines)} lines")
                return lines

        error_msg = f"File with prefix '{file_find}' not found in {directory}"
        self.logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    def write_file(self, directory: str, file_name: str, content: List[str]) -> None:
        file_path = os.path.join(directory, file_name)
        self.logger.info(f"Writing file: {file_path} ({len(content)} lines)")
        with open(file_path, 'w', encoding='latin-1') as f:
            f.writelines(content)
        self.logger.info(f"File successfully written: {file_name}")

    @staticmethod
    def format_line(parts: List[str], original_line: str) -> str:
        base_parts = original_line.split()
        spaces = re.findall(r'\s+', original_line)
        if len(spaces) < len(parts) - 1:
            return original_line  # safe fallback
        new_line = parts[0]
        for i in range(1, len(parts)):
            new_line += parts[i].rjust(len(spaces[i-1]) + len(base_parts[i]))
        return new_line + spaces[-1]

    def adjust_tm(self, entdados: List[str]) -> List[str]:
        self.logger.info("Adjusting TM field to 0 in entdados.dat")
        adjusted = 0
        result = []
        for line in entdados:
            parts = line.split()
            if parts[0] == 'TM':
                if parts[5] != '0':
                    parts[5] = '0'
                    adjusted += 1
                result.append(self.format_line(parts, line))
            else:
                result.append(line)
        self.logger.info(f"{adjusted} TM records adjusted to 0")
        return result

    def map_entdados_ccee(self, file_lines: List[str]) -> Dict[str, Any]:
        self.logger.info("Mapping RE, CE, CI and bars from CCEE entdados.dat")
        re_map, ce_map, ci_map = [], [], []
        barra = {'CI': {}, 'CE': {}}
        for line in file_lines:
            parts = line.split()
            if parts[0] == 'RE':
                re_map.append(parts[1])
            elif parts[0] == 'CE':
                ce_map.append(parts[1])
                barra['CE'][parts[1]] = parts[3]
            elif parts[0] == 'CI':
                ci_map.append(parts[1])
                barra['CI'][parts[1]] = parts[3]
        self.logger.info(f"CCEE mapping: {len(re_map)} RE, {len(ce_map)} CE/CI, {sum(len(v) for v in barra.values())} bars")
        return {'RE': re_map, 'CE': ce_map, 'CI': ci_map, 'BARRA': barra}

    def adjust_di(self, entdados: List[str]) -> List[str]:
        self.logger.info("Adjusting records with code > 800 (commenting 'I')")
        res = ['RE', 'LU', 'FI', 'FH', 'FE', 'FT', 'FC', 'FR']
        commented_lu = []
        adjusted = 0
        result = []
        for line in entdados:
            parts = line.split()
            if parts[0] in res and int(parts[1]) > 800:
                if parts[0] == 'LU' and parts[1] not in commented_lu:
                    commented_lu.append(parts[1])
                    parts[2] = 'I'.rjust(len(parts[2]))
                elif parts[0] != 'LU':
                    parts[2] = 'I'.rjust(len(parts[2]))
                result.append(self.format_line(parts, line))
                adjusted += 1
            else:
                result.append(line)
        self.logger.info(f"{adjusted} records with code > 800 adjusted")
        return result

    def coment_entdados(self, entdados: List[str], map_ccee: Dict[str, Any]) -> List[str]:
        self.logger.info("Commenting records not present in CCEE deck")
        commented = 0
        result = []
        for line in entdados:
            comment = ''
            parts = line.split()
            if parts[0] == 'RD':
                comment = '&'
            elif parts[0] in ['RE', 'LU', 'FI', 'FH', 'FE', 'FT', 'FC', 'FR'] and parts[1] not in map_ccee['RE']:
                comment = '&'
            elif parts[0] == 'CI' and parts[1] not in map_ccee['CI']:
                comment = '&'
            elif parts[0] == 'CE' and parts[1] not in map_ccee['CE']:
                comment = '&'
            if comment:
                commented += 1
            result.append(comment + line)
        self.logger.info(f"{commented} lines commented because they do not exist in CCEE")
        return result

    def uncoment_entdados(self, entdados: List[str], map_ccee: Dict[str, Any]) -> List[str]:
        self.logger.info("Uncommenting records not present in CCEE deck")
        result = []
        for line in entdados:
            parts = line.split()
            if parts[0] in ['&RE', '&LU', '&FI', '&FH', '&FE', '&FT', '&FC', '&FR'] and parts[1] in map_ccee['RE']:
                line = line[1:]  # remove '&'
            elif parts[0] == '&CI' and parts[1] in map_ccee['CI']:
                line = line[1:]   # remove '&'
            elif parts[0] == '&CE' and parts[1] in map_ccee['CE']:
                line = line[1:]   # remove '&'
            result.append(line)
        return result
    
    def adjust_barras(self, entdados: List[str], map_ccee: Dict[str, Dict]) -> List[str]:
        self.logger.info("Adjusting bar voltages according to CCEE mapping")
        adjusted = 0
        result = []
        for line in entdados:
            parts = line.split()
            if parts[0] in ['CI', 'CE'] and parts[1] in map_ccee[parts[0]]:
                old = parts[3]
                parts[3] = map_ccee[parts[0]][parts[1]]
                if old != parts[3]:
                    adjusted += 1
                result.append(self.format_line(parts, line))
            else:
                result.append(line)
        self.logger.info(f"{adjusted} bars with adjusted voltage")
        return result

    def update_load(self, entdados: List[str], df_carga: pd.DataFrame) -> List[str]:
        self.logger.info(f"Updating loads in entdados based on PDO ({len(df_carga)} load records)")
        updated = 0
        result = []
        for line in entdados:
            parts = line.split()
            if parts[0] == 'DP':
                filt = (df_carga['DIA'] == int(parts[2])) & \
                       (df_carga['HORA'] == int(parts[3])) & \
                       (df_carga['MINUTO'] == int(parts[4])) & \
                       (df_carga['SUB'] == int(parts[1]))
                if filt.any():
                    new_load = str(df_carga[filt]['CARGA'].values[0])
                    if parts[-1] != new_load:
                        updated += 1
                    parts[-1] = new_load
                result.append(self.format_line(parts, line))
            else:
                result.append(line)
        self.logger.info(f"{updated} load records updated in entdados")
        return result

    def comment_arq(self, dessem_arq: List[str]) -> List[str]:
        self.logger.info("Commenting INDELET line in dessem.arq")
        commented = 0
        result = []
        for line in dessem_arq:
            if line.strip().upper().startswith('INDELET'):
                result.append('&' + line)
                commented += 1
            else:
                result.append(line)
        self.logger.info(f"{commented} INDELET line(s) commented")
        return result

    def read_load_pdo(self, path: str) -> pd.DataFrame:
        self.logger.info("Reading load data from pdo_sist.dat")
        pdo_file = self.find_file(path, 'pdo_sist.dat')
        data, load = [], False
        current_date = None

        for line in pdo_file:
            words = line.split()
            parts = line.split(';')

            if words and words[0].lower() == 'te':
                current_date = datetime.strptime(words[-1].strip(), "%d/%m/%Y")
                self.logger.debug(f"PDO date: {current_date.strftime('%d/%m/%Y')}")

            if parts[0].strip().lower() == 'iper':
                load = True
                continue

            if load and '-' not in parts[0] and current_date:
                minuto = (int(parts[0].strip()) - 1) * 30
                date_load = current_date + timedelta(minutes=minuto)
                data.append({
                    'PERIODO': int(parts[0].strip()),
                    'DIA': date_load.day,
                    'HORA': date_load.hour,
                    'MINUTO': date_load.minute,
                    'SUB': parts[2].strip(),
                    'CARGA': parts[4].strip()
                })

        df_carga = pd.DataFrame(data)
        original_count = len(df_carga)
        df_carga['MINUTO'] = df_carga['MINUTO'].replace(30, 1)
        df_carga = df_carga[df_carga['PERIODO'] < 49]
        df_carga = df_carga[df_carga['SUB'] != 'FC']
        df_carga['SUB'] = df_carga['SUB'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
        df_carga = df_carga.sort_values(by=['SUB', 'PERIODO', 'HORA', 'MINUTO']).reset_index(drop=True)

        self.logger.info(f"PDO processed: {original_count} → {len(df_carga)} valid records (48h, 4 subsystems)")
        return df_carga

    def cleanup(self, path_ccee: str, path_ons: str) -> None:
        self.logger.info("Starting cleanup of temporary directories")
        try:
            ccee_parent = os.path.dirname(path_ccee)
            if os.path.exists(ccee_parent):
                shutil.rmtree(ccee_parent)
                self.logger.info(f"CCEE directory removed: {ccee_parent}")
        except Exception as e:
            self.logger.warning(f"Failed to remove CCEE directory: {e}")

        try:
            if os.path.exists(path_ons):
                shutil.rmtree(path_ons)
                self.logger.info(f"ONS directory removed: {path_ons}")
        except Exception as e:
            self.logger.warning(f"Failed to remove ONS directory: {e}")


if __name__ == '__main__':
    processor = DessemOnsToCcee()
    processor.run_process()