import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';
import { config } from '../config/env.js';

const adapter = new PrismaPg({ connectionString: config.db.url });
export const prisma = new PrismaClient({ adapter });
