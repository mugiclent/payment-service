import { defineConfig } from 'prisma/config';
import 'dotenv/config';

export default defineConfig({
  schema: './prisma/schema.prisma',
  datasource: {
    url: process.env['DIRECT_DATABASE_URL']!,
    shadowDatabaseUrl: process.env['SHADOW_DATABASE_URL'],
  },
});
