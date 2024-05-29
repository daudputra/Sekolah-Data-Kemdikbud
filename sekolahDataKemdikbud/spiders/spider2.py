import os
import re 
from typing import Any
import scrapy
from scrapy.http import Response
from shapely.geometry import LineString
from ..tools.save_json.save import save_json
from ..tools.s3_token.token import upload_to_s3
from datetime import datetime
from ..log_config import error_logger, links_logger, detail_links_logger


class SpiderBeta(scrapy.Spider):
    name = "spider2"
    allowed_domains = ["sekolah.data.kemdikbud.go.id"]
    start_urls = ["https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/"]
    
    def clean_text(self, text):
        cleaned_text = text.replace('\u201c', '').replace('\u00a0','').replace('\n','').replace('\r', '')
        return cleaned_text
    
    def log_error(self, message):
        error_logger.error(message)

    def log_links(self, message):
        links_logger.info(message)

    def log_detail_links(self, message):
        detail_links_logger.debug(message)

    def parse(self, response):
        self.log_links(self.name)
        if response.status == 503:
            self.log_error("Got 503 Service Unavailable, retrying...")
            request = response.request.copy()
            request.dont_filter = True
            yield request
        else:
            self.log_links("Success!")
            ul_jenjang  = response.css('body > div.container > div > div:nth-child(2) > div > div > div:nth-child(1) > div:nth-child(2) > div > select > option')
            for li_element in ul_jenjang:
                li_text = li_element.css('::text').get()

                links = [
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/066200/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/066000/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/066400/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220400/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220700/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220100/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220500/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220200/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220800/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220600/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/220300/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/226000/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/280200/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/280100/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/280400/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/280300/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/286000/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/286200/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/286100/{li_text}",
                    f"https://sekolah.data.kemdikbud.go.id/index.php/Cpetasebaran/index/286300/{li_text}",
                ]

                
                for link in links:
                        yield scrapy.Request(link, callback=self.parse_detail)





    def parse_detail(self, response):
        if response.status == 503:
            self.log_error("Got 503 Service Unavailable, retrying...")
            request = response.request.copy()
            request.dont_filter = True
            yield request
        else:
            self.log_links("Parsing detail: %s" % response.url)
            data_script = response.xpath("/html/body/div[2]/div/div[3]/script[3]").get()
            pattern = r'<li class="list-group-item text-muted">NPSN : (\d+)</li>' \
                    r'.*?<li class="list-group-item text-info"><a href="(.+?)" target="_blank">(.+?)</a></li>' \
                    r'.*?<li class="list-group-item text-muted"><b>Alamat</b> : (.+?)</li>' \
                    r'.*?L\.marker\(L\.latLng\(([\d.-]+),([\d.-]+)\)\)'

            data_list = re.findall(pattern, data_script, re.DOTALL)
            
            for data in data_list:
                npsn = data[0].strip()
                url_detail_sekolah = data[1].strip()
                nama_sekolah = data[2].strip()
                alamat = data[3].strip()
                latitude = data[4]
                longitude = data[5]
                cordinat = [float(latitude), float(longitude)]


                
                jenjang_pen = response.url.split('/')[-1]       
                kabupaten_value = response.url.split('/')[-2]
                kabupaten = response.xpath(f'//select[@id="kode_kabupaten"]/option[@value="{kabupaten_value}"]/text()').get()

                yield scrapy.Request(url_detail_sekolah, callback=self.parse_detail_sekolah, 
                meta={'npsn': npsn, 'cordinat': cordinat, 'nama_sekolah': nama_sekolah, 'alamat': alamat, 'jenjang_pen': jenjang_pen, "kabupaten": kabupaten}
                )

    def parse_detail_sekolah(self, response):
        if response.status == 503:
            self.log_error("Got 503 Service Unavailable, retrying...")
            request = response.request.copy()
            request.dont_filter = True
            yield request
        else: 
            self.log_detail_links("Parsing detail sekolah: %s" % response.url)
            npsn = response.meta['npsn']
            cordinat = response.meta['cordinat']
            nama_sekolah = response.meta['nama_sekolah']
            alamat = response.meta['alamat']
            jenjang_pen = response.meta['jenjang_pen']
            kabupaten = response.meta['kabupaten']
            



            tag = [
                response.url.split('//')[1].split('/')[0],
                kabupaten,
                jenjang_pen
            ]

            data_name = 'Peta Persebaran Sekolah kemdikbud'.replace(' ', '_').lower()
            kabupaten_json = kabupaten.replace(' ', '_').lower().replace(',','').replace('.','')
            jenjang_pen_json = jenjang_pen.replace(' ', '_').lower()

            title_detail = response.css('#page-top > div.container > div > div:nth-child(1) > h4.page-header::text').get()
            alamat_detail = response.css('#page-top > div.container > div > div:nth-child(1) > h4.page-header > small > font::text').get()  
            akreditasi = response.xpath('//*[@id="page-top"]/div[2]/div/div[3]/div/div/div/ul/li[2]/text()[2]').get().replace(' : ', '')
            operator = response.css('#page-top > div.container > div > div.col-xs-12.col-md-4 > div > div > div > ul > li:nth-child(4) > a::text').get()
            kepala_sekolah = response.css('#page-top > div.container > div > div.col-xs-12.col-md-4 > div > div > div > ul > li:nth-child(3)::text').get()
            kepala_sekolah = self.clean_text(kepala_sekolah)


            # ! data proses pembelajaran
            data_proses_pembelajaran = []
            div_proses_pembelajaran = response.css('#page-top > div.container > div > div.col-xs-12.col-md-5 > ul > li:nth-child(n+3)')
            for div in div_proses_pembelajaran:
                header = div.css('a.text-warning::text').get().replace(' ', '_').lower()
                text = div.css('span.badge.pull-right::text').get()
                data_proses_pembelajaran.append({header: text})


            # ! data akreditasi sekolah
            data_akreditasi_sekolah = []
            data_blocks = response.css('#dataakreditasi > div > div > ul > li')
            for item in data_blocks:
                text = item.css('::text').get()
                if "a" in text:
                    label, value = text.split(" : ")
                    label = label.replace(" ", "_").lower()
                    label = self.clean_text(label)
                    value = value.strip()
                    data_akreditasi_sekolah.append({label: value})

            #  ! rombongan belajar
            data_rombongan_belajar = []
            rombongan_div = response.css('#rombel > div > div > div > div > div table tr:nth-child(n+2)')
            for item in rombongan_div:
                th = item.css('td:nth-child(1)::text, td:nth-child(1) b::text').get()
                td = item.css('td:nth-child(2)::text, td:nth-child(2) b::text').get()
                data_rombongan_belajar.append({'Tingkat': th, 'Jumlah': td})

            # !bansosdata_bansos
            data_bansos = []
            div_tb = response.css('#bansos > div > div > div > div')
            for div in div_tb:
                kategori = div.css('::attr(id)').get()
                ths = div.css('table th::text').getall()
                tds = div.css('table td::text').getall()
                data = {}
                for idx, th in enumerate(ths):
                    heading = th.strip()
                    value = tds[idx].strip() if idx < len(tds) else '' 
                    data[heading] = value
                if any(value != '' for value in data.values()):
                    data_bansos.append({kategori: data})
            # if not data_bansos:
                # data_bansos.append()


            # ! data sarana dan prasarana
            tabs = response.css('#sarpras > div > div div')
            sarana_prasarana = {}
            
            for tab in tabs:
                kategori = tab.css('::attr(id)').get()
                rows = tab.css('table tr') 
                data = []

                if kategori == 'laboratorium' or kategori == 'sanitasi':
                    for row in rows[2:]:  
                        columns = row.css('td')
                        if len(columns) == 6:
                            nama_laboratorium = columns[0].css('::text').get()
                            kondisi = {
                                'Baik': columns[1].css('::text').get(),
                                'Rusak Ringan': columns[2].css('::text').get(),
                                'Rusak Sedang': columns[3].css('::text').get(),
                                'Rusak Berat': columns[4].css('::text').get()
                            }
                            jumlah = columns[5].css('::text').get()
                            data.append({nama_laboratorium: {'kondisi': kondisi, 'jumlah': jumlah}})
                else: 
                    for row in rows[1:]:  
                        columns = row.css('td')
                        if len(columns) == 2:
                            heading = columns[0].css('::text').get().strip()
                            value = columns[1].css('::text').get().strip() if columns[1].css('::text').get() else ''
                            data.append({heading: value})

                sarana_prasarana[kategori] = data


            data_details_sekolah = []
            div_details_sekolah = response.css('#page-top > div.container > div > div:nth-child(4) > div')
            for div in div_details_sekolah:
                    
                sumber_listrik = response.xpath('//*[@id="page-top"]/div[2]/div/div[4]/div/div[3]/text()[4]').get().replace('Sumber Listrik : ', '')
                if sumber_listrik == None:
                    sumber_listrik = None
                akses_internet = div.xpath('//*[@id="page-top"]/div[2]/div/div[4]/div/div[3]/text()[2]').get().replace('Akses Internet : ', '')
                if akses_internet == None:
                    akses_internet = None
                daya_listrik = div.css('i.glyphicon-flash:nth-of-type(3) + font::text').get()
                manajemen_berbasis_sekolah = div.css('i.glyphicon-cog + i.glyphicon-unchecked::attr(class), i.glyphicon glyphicon-cog + i.glyphicon glyphicon-check text-info::attr(class)').get()
                if manajemen_berbasis_sekolah == None:
                    manajemen_sekolah = False
                else:
                    manajemen_sekolah = True
                data_details_sekolah_dalam = {
                    'guru': div.css('i.glyphicon-user + font::text').get(),
                    'siswa_laki_laki': div.css('i.fa-user:nth-of-type(2) + font::text').get(),
                    'siswa_perempuan': div.css('i.fa-user:nth-of-type(3) + font::text').get(),
                    'rombongan_belajar': div.css('i.glyphicon-list-alt + font::text').get(),
                    'penyelenggaraan': div.css('i.glyphicon-globe + font::text').get(),                            
                    'manajemen_berbasis_sekolah': manajemen_sekolah,
                    'semester_data': div.css('i.glyphicon-cog + font::text').get(),
                    'akses_internet': akses_internet,
                    'sumber_listrik': sumber_listrik,
                    'daya_listrik': daya_listrik,
                    'luas_tanah': div.css('i.glyphicon-globe + font::text').get(),
                    'ruang_kelas': div.css('i.glyphicon-stop + font::text').get(),
                    'laboratorium': div.css('i.glyphicon-tint + font::text').get(),
                    'perpustakaan': div.css('i.glyphicon-book + font::text').get(),
                    'sanitasi_siswa': div.css('i.glyphicon-hand-right + font::text').get()
                }
                data_details_sekolah.append(data_details_sekolah_dalam)



            # !data guru
            data_guru = {
                "guru": {},
                "tenaga_kependidikan": {}
            }

            #? Parsing Guru
            guru_tabs = response.css('.page-header:contains("Guru") + .nav.nav-tabs li a::attr(href)').getall()
            for tab in guru_tabs:
                tab_id = tab.strip('#')
                table = response.css(f'div#{tab_id} table')
                if table:
                    headers = table.css('tr:first-child td::text, tr:first-child th::text').getall()
                    rows = table.css('tr')[1:]
                    tab_data = []

                    for row in rows:
                        columns = row.css('td::text, td b::text, td strong::text').getall()
                        entry = {}
                        for i in range(len(columns)):
                            if i < len(headers):
                                entry[headers[i].strip()] = columns[i].strip()
                            else:
                                entry[f'Column {i+1}'] = columns[i].strip()
                        tab_data.append(entry)

                    data_guru["guru"][tab_id] = tab_data

            #? Parsing Tenaga Kependidikan
            ptk_tabs = response.css('.page-header:contains("Tenaga Kependidikan") + .nav.nav-tabs li a::attr(href)').getall()
            for tab in ptk_tabs:
                tab_id = tab.strip('#')
                table = response.css(f'div#{tab_id} table')
                if table:
                    headers = table.css('tr:first-child td::text, tr:first-child th::text').getall()
                    rows = table.css('tr')[1:] 
                    tab_data = []

                    for row in rows:
                        columns = row.css('td::text, td b::text, td strong::text').getall()
                        entry = {}
                        for i in range(len(columns)):
                            if i < len(headers):
                                entry[headers[i].strip()] = columns[i].strip()
                            else:
                                entry[f'Column {i+1}'] = columns[i].strip()
                        tab_data.append(entry)

                    data_guru["tenaga_kependidikan"][tab_id] = tab_data


        # ! data siswa
            data_siswa = {
                "siswa": {
                    "siswatingkat": [],
                    "siswaagama": [],
                    "siswaumur": [],
                    "siswajeniskelamin": []
                },
                "siswa_mengulang": {},  
                "siswa_baru": {}, 
                "siswa_lulus": {} 
            }

            tabs_mapping = {
                "Siswa": "siswa",
                "Siswa Mengulang": "siswa_mengulang",
                "Siswa Baru": "siswa_baru",
                "Siswa Lulus": "siswa_lulus"
            }

            for header, key in tabs_mapping.items():
                tabs = response.css(f'div#pd h4.page-header:contains("{header}") + .nav.nav-tabs li a::attr(href)').getall()
                for tab in tabs:
                    tab_id = tab.strip('#')
                    table = response.css(f'div#{tab_id} table')
                    if table:
                        headers = table.css('tr:first-child td::text, tr:first-child th::text').getall()
                        rows = table.css('tr')[1:]
                        tab_data = []

                        for row in rows:
                            columns = row.css('td::text, td b::text, td strong::text').getall()
                            entry = {}
                            for i in range(len(columns)):
                                entry[headers[i].strip()] = columns[i].strip()
                            tab_data.append(entry)

                        data_siswa[key][tab_id] = tab_data

            # Menghapus kunci-kunci yang tidak diinginkan
            unwanted_keys = ["siswamengulangtingkat", "siswamengulangumur", "siswamengulangjeniskelamin", "siswabaruumur",
                            "siswabarujeniskelamin", "lulusanumur", "lulusanjeniskelamin"]
            for key in unwanted_keys:
                if key in data_siswa["siswa"]:
                    del data_siswa["siswa"][key]



            path = os.path.join('json', kabupaten_json, jenjang_pen_json)
            os.makedirs(path, exist_ok=True)
            filename = f'{kabupaten_json}_{jenjang_pen_json}_{npsn}_.json'

            local_path = f'~/engine-daud/Sekolah-Data-Kemdikbud/sekolahDataKemdikbud/json/{kabupaten_json}/{jenjang_pen_json}/{filename}'
            s3path = f's3://ai-pipeline-raw-data/data/data_statistics/kemendikbud/{data_name}/json/{kabupaten_json}/{jenjang_pen_json}/{filename}'

            data = {
                "link": response.url,
                "domain": response.url.split('//')[1].split('/')[0],
                "tag": tag,
                "crawling_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "crawling_time_epoch": int(datetime.now().timestamp()),
                "path_data_raw": s3path,
                'kabupaten': kabupaten,
                'jenjang': jenjang_pen,
                'NSPN': npsn,
                'nama_sekolah': nama_sekolah,
                'alamat': alamat,
                'coordinate': cordinat,
                'details_sekolah' : {
                    'title': title_detail,
                    'alamat' : alamat_detail,
                    'akreditasi' : akreditasi,
                    'kepala_sekolah' : kepala_sekolah,
                    'operator' : operator,
                    'detail_sekolah' : data_details_sekolah, 
                    'proses_pembelajaran' : data_proses_pembelajaran,
                    'details_sekolah' : {
                        'siswa': data_siswa,
                        'guru' :data_guru,
                        'rombongan_belajar' : data_rombongan_belajar,
                        'sarana_dan_prasarana' : sarana_prasarana,
                        'nilai_akreditasi' : data_akreditasi_sekolah,
                        'program_pembangunan_bantuan' : data_bansos,
                    }
                }
            }
            save_json(data, os.path.join(path, filename))
            self.log_detail_links(f"Success save {filename}")
            try:
                upload_to_s3(local_path, s3path.replace('s3://', ''))
                self.log_detail_links(f"Success upload {filename} to s3")
            except Exception as e:
                self.log_detail_links(f"Error upload to s3: {str(e)}")
                
            

