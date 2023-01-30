import { errorRegex } from "./constants.js";

export const isErrorPresent = (str) => errorRegex.test(str);