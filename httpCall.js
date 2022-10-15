import fetch from 'node-fetch';

export default async function httpCall(url, options) {
    const response = await fetch(url, options);

    if (!response.ok) {
        const message = await response.text();
        throw new Error(message, { response });
    }
}