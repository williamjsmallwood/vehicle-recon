const ftp = require('basic-ftp');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const { parse } = require('csv-parse/sync');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Add more stores here as you onboard them
const STORES = [
  {
    storeCode: 'L499',
    storeName: 'Elder Ford Tampa',
    filename: 'L499_inventory.csv',   // ask vAuto to name it this
    remotePath: '/L499_inventory.csv'
  },
  // {
  //   storeCode: 'L500',
  //   storeName: 'Next Store Name',
  //   filename: 'L500_inventory.csv',
  //   remotePath: '/L500_inventory.csv'
  // },
];

async function syncStore(client, store) {
  console.log(`\n📦 Syncing ${store.storeName} (${store.storeCode})...`);

  try {
    await client.downloadTo(store.filename, store.remotePath);
    console.log(`✅ File downloaded for ${store.storeCode}`);

    const records = parse(fs.readFileSync(store.filename, 'utf8'), {
      columns: true,
      skip_empty_lines: true
    });

    console.log(`📋 ${records.length} rows found`);

    const vehicles = records
      .map(r => ({
        vin:          r['VIN']          || '',
        stock_number: r['Stock #']      || r['StockNumber'] || '',
        year:         parseInt(r['Year'])  || null,
        make:         r['Make']         || '',
        model:        r['Model']        || '',
        miles:        parseInt(r['Miles'] || r['Mileage']) || 0,
        color:        r['Ext Color']    || r['Color'] || '',
        store_code:   store.storeCode,        // tags every vehicle with the store
        store_name:   store.storeName,
      }))
      .filter(v => v.vin.length > 5);

    console.log(`🚗 ${vehicles.length} valid vehicles for ${store.storeCode}`);

    const { error } = await supabase
      .from('vehicles')
      .upsert(vehicles, { onConflict: 'vin' });

    if (error) throw error;
    console.log(`✅ Synced ${vehicles.length} vehicles for ${store.storeName}`);

  } catch (err) {
    if (err.message.includes('550')) {
      console.log(`⚠️ No file found for ${store.storeCode} - skipping`);
      return; // skip this store, don't fail the whole job
    }
    throw err;
  } finally {
    if (fs.existsSync(store.filename)) fs.unlinkSync(store.filename);
  }
}

async function sync() {
  const client = new ftp.Client();
  client.ftp.verbose = true;

  try {
    await client.access({
      host: process.env.FTP_HOST,
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: false
    });

    console.log('✅ FTP connected');

    // Sync all stores sequentially
    for (const store of STORES) {
      await syncStore(client, store);
    }

    console.log('\n✅ All stores synced successfully');

  } catch (err) {
    console.error('❌ Sync failed:', err.message);
    process.exit(1);
  } finally {
    client.close();
  }
}

sync();        stock_number: r['Stock #']      || r['StockNumber'] || '',
        year:         parseInt(r['Year'])  || null,
        make:         r['Make']         || '',
        model:        r['Model']        || '',
        miles:        parseInt(r['Miles'] || r['Mileage']) || 0,
        color:        r['Ext Color']    || r['Color'] || '',
      }))
      .filter(v => v.vin.length > 5);

    console.log(`🚗 ${vehicles.length} valid vehicles to sync`);

    const { error } = await supabase
      .from('vehicles')
      .upsert(vehicles, { onConflict: 'vin' });

    if (error) throw error;
    console.log(`✅ Successfully synced ${vehicles.length} vehicles to Supabase`);

  } catch (err) {
    console.error('❌ Sync failed:', err.message);
    process.exit(1); // causes GitHub Action to show as failed
  } finally {
    client.close();
    if (fs.existsSync('inventory.csv')) fs.unlinkSync('inventory.csv');
  }
}

sync();
