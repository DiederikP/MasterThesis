import SimplxrAPI as dio
%matplotlib inline

dio._baseUrl = 'http://localhost:6541'

%%time

# Test 1: 
# alle data uit buurt 00030000 en scenario 'MM'
df1 = dio.getData(
    sources = ['ABF_WoningVoorraad_B'],
    filters = {
        'BI_Buurt': ['00030000'],
        'BI_Scenario': ['MM'],
        
    },
    detailLevels = ['BI_Buurt'],
)
print("Number of lines:", len(df2))

# Number of lines: 2028
# CPU times: user 0 ns, sys: 0 ns, total: 0 ns
# Wall time: 559 ms

%%time

# Test 2: 
# per datum de woonvoorraad in buurt 00030000
df2 = dio.getData(
    sources = ['ABF_WoningVoorraad_B'],
    filters = {
        'BI_Buurt': ['00030000'],
        'BI_Scenario': ['MM'],
        
    },
    detailLevels = ['BI_Buurt'],
)
cnts = df2.groupby('valueDate').WoningVoorraad.sum()
print("Number of lines:", len(cnts))

# Number of lines: 26
# CPU times: user 0 ns, sys: 0 ns, total: 0 ns
# Wall time: 527 ms

%%time

# Test 3:
# per datum en scenario de woonvoorraad in gemeente
df3 = dio.getData(
    sources = ['ABF_WoningVoorraad_B'],
    filters = {
        'BI_Gemeente': ['1904'],
        'BI_Scenario': ['MM'],
    },
    detailLevels = ['BI_Buurt'],
)
cnts = df3.groupby('valueDate').WoningVoorraad.sum()
print("Number of lines:", len(cnts))

# CPU times: user 0 ns, sys: 296 ms, total: 296 ms
# Wall time: 12.6 s

%%time

# Test 4:
# per datum de grootste woonvoorraad per buurt in de gemeente '1904'
df4 = dio.getData(
    sources = ['ABF_WoningVoorraad_B'],
    filters = {
        'BI_Gemeente': ['1904'],
        'BI_Scenario': ['MM'],
    },
    detailLevels = ['BI_Buurt'],
)
cnts = df4.groupby('valueDate').WoningVoorraad.max()
print("Number of lines:", len(cnts))

# Number of lines: 26
# CPU times: user 108 ms, sys: 168 ms, total: 276 ms
# Wall time: 12.7 s

%%time

# Test 6:
# t4 met een extra restrictie. 
df5 = dio.getData(
    sources = ['ABF_WoningVoorraad_B'],
    filters = {
        'BI_Gemeente': ['1900'],
        'BI_Scenario': ['HM'],
        'BI_WoningType': ['MGW'],
    },
    detailLevels = ['BI_Buurt'],
)
cnts = df5.groupby('valueDate').WoningVoorraad.sum()
print("Number of lines:", len(cnts))

# Number of lines: 26
# CPU times: user 228 ms, sys: 0 ns, total: 228 ms
# Wall time: 8.94 s