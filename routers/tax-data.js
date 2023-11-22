// const { Router } = require("express");
// const { query } = require("../lib/db");
// const {
//     taxCategory,
//     state,
//     taxData
// } = require("../lib/table");

// const taxDataRouter = Router();

// taxDataRouter.get("/taxcategory", async (req, res) => {
//     try {
//         const data = await query(`
//         SELECT UNIQUE category_code, category_name
//         FROM ${taxCategory}
//         ORDER BY category_code ASC
//       `);
//         res.json(data);
//     } catch (err) {
//         console.error("Failed to fetch tax categories");
//         console.error(err);
//         res.status(500).json({ error: "Failed to fetch tax categories" });
//     }
// });

// taxDataRouter.get("/", async (req, res) => {
//     const stateCode = parseInt(req.query.stateCode);
//     if (!stateCode || Number.isNaN(stateCode))
//         return res.status(400).json({ error: "Invalid state code" });

//     try {
//         const data = await query(
//             `
//             SELECT 
//                 TD.year, 
//                 S.state_name, 
//                 TC.category_name, 
//                 SUM(TD.amount) AS total_tax_revenue
//             FROM 
//                 ${taxData} TD
//             INNER JOIN 
//                 ${state} S ON S.state_code = TD.state_code
//             INNER JOIN 
//                 ${taxCategory} TC ON TC.category_code = TD.category_code
//             WHERE 
//                 S.state_code = :stateCode AND
//                 TD.amount IS NOT NULL
//             GROUP BY 
//                 TD.year, S.state_name, TC.category_name
//             ORDER BY 
//                 TD.year ASC, S.state_name ASC
//         `,
//             { stateCode }
//         );

//         const map = new Map();
//         for (let row of data) {
//             if (map.has(row.year)) {
//                 map.get(row.year)[row.categoryName] = row.totalTaxRevenue;
//             } else {
//                 map.set(row.year, {
//                     year: row.year,
//                     [row.categoryName]: row.totalTaxRevenue
//                 });
//             }
//         }
//         const year2021Data = map.get(2021);
//         if (year2021Data) {
//             delete year2021Data["Taxes, NEC"]; // Replace "CategoryName" with the actual category key you want to remove
//             // Optionally, if you want to update the map after deletion
//             map.set(2021, year2021Data);
//         }
//         res.json(Array.from(map.values()));
//     } catch (err) {
//         console.error("Failed to fetch tax data");
//         console.error(err);
//         res.status(500).json({ error: "Failed to fetch tax data" });
//     }
// });


// taxDataRouter.get("/top-5-taxcategory", async(req, res) => {
//     const stateCode = parseInt(req.query.stateCode);
//   if (!stateCode || Number.isNaN(stateCode))
//     return res.status(400).json({ error: "Invalid state code" });

//   const startYear = parseInt(req.query.startYear) || 2012;
//   const endYear = parseInt(req.query.endYear) || 2021;
//   const toptaxcategoryData = await query(
//     `
//             SELECT 
//             TD.category_code,
//             TC.category_name,
//             SUM(TD.amount) AS total_amount
//         FROM 
//         ${taxData} TD
//         JOIN 
//         ${taxCategory} TC ON TD.category_code = TC.category_code
//         WHERE 
//             TD.state_code = : stateCode AND 
//             TD.year >= : startYear AND
//             TD.year <= : endYear
//         GROUP BY 
//             TD.category_code, TC.category_name
//         ORDER BY 
//             total_amount DESC
//             FETCH FIRST 5 ROWS ONLY
//     `,
//     { stateCode, startYear, endYear }
//   );

//   if (toptaxcategoryData.length == 0) {
//     return res.status(404).json({ error: "No data found for the state" });
//   }

//   const map = new Map();
//   for (let row of toptaxcategoryData) {
//     const { categoryName, totalAmount } = row;
//     map.set(categoryName, totalAmount);
//   }

//   const payload = [["Category Name", "Total Amount"]];
//   for (let key of map.keys()) {
//     payload.push([key, map.get(key)]);
//   }

//   res.json(payload);
// });

// module.exports = taxDataRouter;



const { Router } = require("express");
const { query } = require("../lib/db");
const {
    taxCategory,
    state,
    taxData,
    covidData
} = require("../lib/table");

const taxDataRouter = Router();

// ... other endpoints ...

taxDataRouter.get("/", async (req, res) => {
    const stateCode = parseInt(req.query.stateCode);
    const startYear = parseInt(req.query.startYear) || 2012;
    const endYear = parseInt(req.query.endYear) || 2021;

    if (!stateCode || Number.isNaN(stateCode))
        return res.status(400).json({ error: "Invalid state code" });

    try {
        const [taxcategory,taxDataResults, allCovidData] = await Promise.all([
            query(`
                SELECT UNIQUE category_code, category_name
                FROM ${taxCategory}
                ORDER BY category_code ASC
            `),
            query(
                `
                SELECT 
                    TD.year, 
                    TC.category_name, 
                    SUM(TD.amount) AS total_tax_revenue
                FROM 
                    ${taxData} TD
                INNER JOIN 
                    ${state} S ON S.state_code = TD.state_code
                INNER JOIN 
                    ${taxCategory} TC ON TC.category_code = TD.category_code
                WHERE 
                    S.state_code = :stateCode AND
                    TD.year >= :startYear AND
                    TD.year <= :endYear AND
                    TD.amount IS NOT NULL
                GROUP BY 
                    TD.year, TC.category_name
                ORDER BY 
                    TD.year ASC, TC.category_name ASC
                `,
                { stateCode, startYear, endYear }
            ),
            query(
                `
                SELECT year, SUM(count_confirmed_cases) as count_cases
                FROM ${covidData}
                WHERE state_code = :stateCode
                AND year >= :startYear
                AND year <= :endYear
                GROUP BY year, state_code
                `,
                { stateCode, startYear, endYear }
            )
        ]);
        let alltaxcategories = taxcategory.map(tc => tc.categoryName)
        const taxMap = new Map();
        for (let row of taxDataResults) {
            const { year, categoryName, totalTaxRevenue } = row;
            if (taxMap.has(year)) {
                taxMap.get(year)[categoryName]= totalTaxRevenue;
            }else{
                let obj = {};
                for (let name of alltaxcategories) {
                obj[name] = 0;
                }
                obj[categoryName] = totalTaxRevenue;
                taxMap.set(year, obj);
            }
            }
        

            const covidDataMap = new Map();
            for (let row of allCovidData) {
              const { year, countCases } = row;
              covidDataMap.set(year, countCases);
            }

        let payload = [["Year", "Confirmed COVID-19 Cases", ...alltaxcategories]];
        for (let year of taxMap.keys()) {
            payload.push([
                year,
                covidDataMap.has(year) ? covidDataMap.get(year) : 0,
                ...Object.values(taxMap.get(year))
            ]);
        }

            const headers = payload[0];
            const arrayOfMaps = payload.slice(1).map(row => {
            const rowMap = {};
            headers.forEach((header, index) => {
                rowMap[header] = row[index];
            });
            return rowMap;
            });

        res.json(arrayOfMaps);
    } catch (err) {
        console.error("Failed to fetch combined tax and COVID data");
        console.error(err);
        res.status(500).json({ error: "Failed to fetch combined tax and COVID data" });
    }
});

taxDataRouter.get("/top-5-taxcategory", async(req, res) => {
    const stateCode = parseInt(req.query.stateCode);
  if (!stateCode || Number.isNaN(stateCode))
    return res.status(400).json({ error: "Invalid state code" });

  const startYear = parseInt(req.query.startYear) || 2012;
  const endYear = parseInt(req.query.endYear) || 2021;
  const toptaxcategoryData = await query(
    `
            SELECT 
            TD.category_code,
            TC.category_name,
            SUM(TD.amount) AS total_amount
        FROM 
        ${taxData} TD
        JOIN 
        ${taxCategory} TC ON TD.category_code = TC.category_code
        WHERE 
            TD.state_code = : stateCode AND 
            TD.year >= : startYear AND
            TD.year <= : endYear
        GROUP BY 
            TD.category_code, TC.category_name
        ORDER BY 
            total_amount DESC
            FETCH FIRST 5 ROWS ONLY
    `,
    { stateCode, startYear, endYear }
  );

  if (toptaxcategoryData.length == 0) {
    return res.status(404).json({ error: "No data found for the state" });
  }

  const map = new Map();
  for (let row of toptaxcategoryData) {
    const { categoryName, totalAmount } = row;
    map.set(categoryName, totalAmount);
  }

  const payload = [["Category Name", "Total Amount"]];
  for (let key of map.keys()) {
    payload.push([key, map.get(key)]);
  }

  res.json(payload);
});

module.exports = taxDataRouter;


module.exports = taxDataRouter;



