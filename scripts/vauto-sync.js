const ftp = require('basic-ftp');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const { parse } = require('csv-parse/sync');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

async function sync() {
  const client = new ftp.Client();
  client.ftp.verbose = true; // logs FTP activity, helpful for debugging

  try {
    await client.access({
      host: process.env.FTP_HOST,
      user: process.env.FTP_USER,
      password: process.env.FTP_PASS,
      secure: false
    });

    console.log('✅ FTP connected');
    await client.downloadTo('inventory.csv', '/htdocs/inventory.csv');
    console.log('✅ File downloaded');

    const records = parse(fs.readFileSync('inventory.csv', 'utf8'), {
      columns: true,
      skip_empty_lines: true
    });

    console.log(`📦 ${records.length} rows found in file`);

    const vehicles = records
      .map(r => ({
        vin:          r['VIN']          || '',
        stock_number: r['Stock #']      || r['StockNumber'] || '',
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
